package forcesleep

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"

	"github.com/golang/glog"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	ProjectSleepQuotaName = "force-sleep"
	ProjectSortAnnotation = "openshift.io/project-sort-index"
)

var forceSleepQuota = &corev1.ResourceQuota{
	ObjectMeta: metav1.ObjectMeta{
		Name: ProjectSleepQuotaName,
	},
	Spec: corev1.ResourceQuotaSpec{
		Hard: corev1.ResourceList{
			"pods": *resource.NewMilliQuantity(0, resource.DecimalSI),
		},
	},
}

type SleeperConfig struct {
	Quota              time.Duration
	Period             time.Duration
	SleepSyncPeriod    time.Duration
	ProjectSleepPeriod time.Duration
	SyncWorkers        int
	TermQuota          resource.Quantity
	NonTermQuota       resource.Quantity
	DryRun             bool
}

type Sleeper struct {
	config        *SleeperConfig
	resourceStore *cache.ResourceStore
	stopChan      <-chan struct{}
}

func NewSleeper(sc *SleeperConfig, rs *cache.ResourceStore) *Sleeper {
	ctrl := &Sleeper{
		config:        sc,
		resourceStore: rs,
	}
	return ctrl
}

// Main function for controller
func (s *Sleeper) Run(stopChan <-chan struct{}) {
	s.stopChan = stopChan

	// Spawn a goroutine to run project sync
	go wait.Until(s.Sync, s.config.SleepSyncPeriod, stopChan)

}

// Spawns goroutines to sync projects
func (s *Sleeper) Sync() {
	glog.V(1).Infof("Force-sleeper: Running project sync")
	projects, err := s.resourceStore.ProjectList.List(labels.Everything())
	if err != nil {
		glog.Errorf("Force-sleeper: Error getting projects for sync: %s", err)
		return
	}
	sort.Sort(Projects(projects))

	namespaces := make(chan string, len(projects))
	for i := 1; i <= s.config.SyncWorkers; i++ {
		go s.startWorker(namespaces)
	}
	for _, namespace := range projects {
		namespaces <- namespace.ObjectMeta.Name
	}
	close(namespaces)
}

func (s *Sleeper) startWorker(namespaces <-chan string) {
	for namespace := range namespaces {
		s.syncProject(namespace)
	}
}

// Syncs a project and determines if force-sleep is needed
func (s *Sleeper) syncProject(namespace string) {
	glog.V(2).Infof("Force-sleeper: Syncing project( %s )", namespace)
	isAsleep, err := s.resourceStore.IsAsleep(namespace)
	if err != nil {
		glog.Errorf("Force-sleeper: Error: %s", err)
		return
	}
	project, err := s.resourceStore.ProjectList.Get(namespace)
	if err != nil {
		glog.Errorf("Force-sleeper: Error: %s", err)
		return
	}
	var lastSleepTime time.Time
	if lastSleepTimeStr, ok := project.Annotations[cache.ProjectLastSleepTime]; ok {
		if lastSleepTimeStr == "0001-01-01 00:00:00 +0000 UTC" {
			lastSleepTime = time.Time{}
		} else {
			lastSleepTime, err = time.Parse(time.RFC3339, lastSleepTimeStr)
			if err != nil {
				glog.Errorf("Force-sleeper: %s", err)
				return
			}
		}
	}
	if isAsleep {
		if !lastSleepTime.IsZero() {
			if time.Since(lastSleepTime) < s.config.ProjectSleepPeriod {
				return
			}
			// Wake the project if project has been asleep longer than force-sleep period
			err := s.wakeProject(namespace)
			if err != nil {
				// TODO: If failed, undo whatever has been done so project is not left in half-sleep?
				s.cleanupOnFailure(namespace)
				return
			}
		}
		glog.V(2).Infof("Force-sleeper: Project( %s )sync complete", namespace)
		return
	} else {
		// Iterate through pods to calculate runtimes
		pods, err := s.resourceStore.PodList.Pods(namespace).List(labels.Everything())
		if err != nil {
			glog.Errorf("Force-sleeper: Error: %s", err)
			return
		}
		nowTime := time.Now()
		termQuotaSecondsConsumed := 0.0
		nonTermQuotaSecondsConsumed := 0.0
		for _, pod := range pods {
			if pod.Status.Phase == corev1.PodRunning {
				thisPodRuntime := nowTime.Sub(pod.Status.StartTime.Time).Seconds()
				memoryLimit := s.memoryQuota(pod)
				quotaSeconds := getQuotaSeconds(thisPodRuntime, getMemoryRequested(pod), memoryLimit)
				if isTerminating(pod) {
					termQuotaSecondsConsumed += quotaSeconds
				} else {
					nonTermQuotaSecondsConsumed += quotaSeconds
				}
			}
		}
		deadPodRuntimeInProj := 0.0
		if dprt, ok := project.Annotations[cache.ProjectDeadPodsRuntimeAnnotation]; ok {
			deadPodRuntimeInProj, err = strconv.ParseFloat(dprt, 64)
			if err != nil {
				glog.Errorf("Force-sleeper: %s", err)
				return
			}
		}

		quotaSecondsConsumed := math.Max(termQuotaSecondsConsumed, nonTermQuotaSecondsConsumed)
		quotaSecondsConsumed += deadPodRuntimeInProj

		if quotaSecondsConsumed > s.config.Quota.Seconds() {
			// Project-level sleep
			glog.V(2).Infof("Force-sleeper: Project( %s )over quota! (%+vs/%+vs), applying force-sleep...", namespace, quotaSecondsConsumed, s.config.Quota.Seconds())
			err = s.applyProjectSleep(namespace, nowTime)
			if err != nil {
				// TODO: If failed, undo whatever has been done so project is not left in half-sleep?
				s.cleanupOnFailure(namespace)
				return
			}
		}

		// Project sort index:
		err = s.updateProjectSortIndex(namespace, quotaSecondsConsumed)
		if err != nil {
			glog.Errorf("Force-sleeper: error: %s", err)
			return
		}
		glog.V(2).Infof("Force-sleeper: Project( %s )sync complete", namespace)
	}
}

// applyProjectSleep creates force-sleep quota in namespace, scales scalable objects, and adds necessary annotations
func (s *Sleeper) applyProjectSleep(namespace string, sleepTime time.Time) error {
	// Log only in DryRun, do not make any quota, idling, annotations, or scaling changes in dry-run mode
	if s.config.DryRun {
		glog.V(2).Infof("Force-sleeper DryRun: Project( %s )would be force-slept now, but DryRun=True, logging only", namespace)
		return nil
	}
	// Add force-sleep annotation to project
	err := s.addNamespaceSleepTimeAnnotation(namespace, sleepTime)
	if err != nil {
		return err
	}
	// Add force-sleep resource quota to object
	failed := false
	quotaInterface := s.resourceStore.KubeClient.CoreV1().ResourceQuotas(namespace)
	_, err = quotaInterface.Create(forceSleepQuota)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: %s", err)
	}
	err = s.resourceStore.CreateOrUpdateIdler(namespace, false)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: %s", err)
	}
	// Now clear any DeadPodRuntimeAnnotation
	project, err := s.resourceStore.ProjectList.Get(namespace)
	pCopy := project.DeepCopy()
	delete(pCopy.Annotations, cache.ProjectDeadPodsRuntimeAnnotation)
	_, err = s.resourceStore.KubeClient.CoreV1().Namespaces().Update(pCopy)
	if err != nil {
		glog.Errorf("Force-sleeper: Error: %s", err)
	}

	if failed {
		return fmt.Errorf("Force-sleeper: Failed to apply project sleep in project( %s )", namespace)
	}
	return nil
}

// cleanupOnFailure attempts to undo any hibernation-component changes to a project
func (s *Sleeper) cleanupOnFailure(namespace string) {
	failed := false
	err := s.removeNamespaceSleepTimeAnnotation(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error removing sleepTime annotation in project( %s ): %s", namespace, err)
	}
	err = s.resourceStore.KubeClient.CoreV1().ResourceQuotas(namespace).Delete(cache.ProjectSleepQuotaName, &metav1.DeleteOptions{})
	if err != nil {
		if !kerrors.IsNotFound(err) {
			failed = true
			glog.Errorf("Force-sleeper: Error removing force-sleep quota from project( %s ): %s", namespace, err)
		}
	}
	err = s.resourceStore.IdlersClient.Idlers(namespace).Delete(cache.HibernationIdler, &metav1.DeleteOptions{})
	if err != nil {
		if !kerrors.IsNotFound(err) {
			failed = true
			glog.Errorf("Force-sleeper: Error removing hibernation idler from project( %s ): %s", namespace, err)
		}
	}
	if failed {
		glog.Errorf("Force-sleeper: Error cleaning up project( %s )after failure to apply force-sleep", namespace)
	}
}

// Adds a cache.ProjectLastSleepTime annotation to a namespace in the cluster
func (s *Sleeper) addNamespaceSleepTimeAnnotation(namespace string, sleepTime time.Time) error {
	ns, err := s.resourceStore.ProjectList.Get(namespace)
	if err != nil {
		return err
	}
	nsCopy := ns.DeepCopy()
	nsCopy.Annotations[cache.ProjectLastSleepTime] = sleepTime.Format(time.RFC3339)
	_, err = s.resourceStore.KubeClient.CoreV1().Namespaces().Update(nsCopy)
	if err != nil {
		return err
	}
	return nil
}

// removeNamespaceSleepTimeAnnotation removes an annotation from a namespace
func (s *Sleeper) removeNamespaceSleepTimeAnnotation(namespace string) error {
	project, err := s.resourceStore.ProjectList.Get(namespace)
	if err != nil {
		return err
	}
	projectCopy := project.DeepCopy()
	projectCopy.Annotations[cache.ProjectLastSleepTime] = time.Time{}.String()
	_, err = s.resourceStore.KubeClient.CoreV1().Namespaces().Update(projectCopy)
	if err != nil {
		return err
	}
	return nil
}

// This function checks if a project needs to wake up and if so, takes the actions to do that
func (s *Sleeper) wakeProject(namespace string) error {
	project, err := s.resourceStore.ProjectList.Get(namespace)
	if err != nil {
		return err
	}
	failed := false
	var plst time.Time
	plstStr := project.Annotations[cache.ProjectLastSleepTime]
	plst, err = time.Parse(time.RFC3339, plstStr)
	if err != nil {
		return err
	}
	if plst.IsZero() {
		failed = true
		glog.Errorf("Force-sleeper: Error: was expecting a cache.ProjectLastSleepTime annotation on project( %s )", namespace)
	}
	if time.Since(plst) < s.config.ProjectSleepPeriod {
		return nil
	}
	// First remove the force-sleep pod count resource quota from the project
	glog.V(2).Infof("Force-sleeper: Removing sleep quota for project( %s )", namespace)
	quotaInterface := s.resourceStore.KubeClient.CoreV1().ResourceQuotas(namespace)
	err = quotaInterface.Delete(ProjectSleepQuotaName, &metav1.DeleteOptions{})
	if err != nil {
		if !kerrors.IsNotFound(err) {
			failed = true
			glog.Errorf("Force-sleeper: Error removing sleep quota in project( %s ): %s", namespace, err)
		}
		glog.V(2).Infof("Force-sleeper: Sleep quota not found in project( %s ), could not remove", namespace)
	}
	// Remove sleep time annotation and update idler with triggerServiceNames for auto-unidling
	err = s.removeNamespaceSleepTimeAnnotation(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error removing project( %s )sleep time annotation: %s", namespace, err)
	}
	glog.V(2).Infof("Force-sleeper: Adding project( %s )TriggerServiceNames for idling", namespace)
	err = s.resourceStore.CreateOrUpdateIdler(namespace, true)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error: %s", err)
	}

	project, err = s.resourceStore.ProjectList.Get(namespace)
	if err != nil {
		return err
	}
	projectCopy := project.DeepCopy()
	projectCopy.Annotations[cache.ProjectLastSleepTime] = time.Time{}.String()
	_, err = s.resourceStore.KubeClient.CoreV1().Namespaces().Update(projectCopy)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error: %s", err)
	}
	if failed {
		return fmt.Errorf("Failed to wake project( %s )", namespace)
	}
	return nil
}

func (s *Sleeper) memoryQuota(pod *corev1.Pod) resource.Quantity {
	// Get project memory quota
	if isTerminating(pod) {
		return s.config.TermQuota
	} else {
		return s.config.NonTermQuota
	}
}

func (s *Sleeper) updateProjectSortIndex(namespace string, quotaSeconds float64) error {
	project, err := s.resourceStore.ProjectList.Get(namespace)
	if err != nil {
		return err
	}
	projectCopy := project.DeepCopy()
	// Projects closer to force-sleep will have a lower index value
	sortIndex := -1 * quotaSeconds
	projectCopy.Annotations[ProjectSortAnnotation] = strconv.FormatFloat(sortIndex, 'f', -1, 64)
	_, err = s.resourceStore.KubeClient.CoreV1().Namespaces().Update(projectCopy)
	if err != nil {
		glog.Errorf("Force-sleeper: %s", err)
	}
	return nil
}

func isTerminating(pod *corev1.Pod) bool {
	terminating := false
	if (pod.Spec.RestartPolicy != corev1.RestartPolicyAlways) && (pod.Spec.ActiveDeadlineSeconds != nil) {
		terminating = true
	}
	return terminating
}

func getMemoryRequested(pod *corev1.Pod) resource.Quantity {
	return pod.Spec.Containers[0].Resources.Limits["memory"]
}
