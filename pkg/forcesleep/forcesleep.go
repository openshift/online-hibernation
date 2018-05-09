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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	kclient "k8s.io/client-go/kubernetes"
)

const (
	ProjectSleepQuotaName = "force-sleep"
	ProjectSortAnnotation = "openshift.io/project-sort-index"
)

type SleeperConfig struct {
	Quota              time.Duration
	Period             time.Duration
	SleepSyncPeriod    time.Duration
	ProjectSleepPeriod time.Duration
	SyncWorkers        int
	Exclude            map[string]bool
	TermQuota          resource.Quantity
	NonTermQuota       resource.Quantity
	DryRun             bool
}

type Sleeper struct {
	config            *SleeperConfig
	resourceStore     *cache.ResourceStore
	projectSleepQuota *corev1.ResourceQuota
	RESTMapper        apimeta.RESTMapper
	stopChan          <-chan struct{}
}

func NewSleeper(sc *SleeperConfig, kc kclient.Interface, rs *cache.ResourceStore) *Sleeper {
	ctrl := &Sleeper{
		config: sc,
		projectSleepQuota: &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name: ProjectSleepQuotaName,
			},
			Spec: corev1.ResourceQuotaSpec{
				Hard: corev1.ResourceList{
					"pods": *resource.NewMilliQuantity(0, resource.DecimalSI),
				},
			},
		},
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
	if isAsleep {
		project, err := s.resourceStore.ProjectList.Get(namespace)
		if err != nil {
			glog.Errorf("Force-sleeper: Error: %s", err)
			return
		}
		var lastSleepTime time.Time
		//Check if quota doesn't exist and should
		if lastSleepTimeStr, ok := project.Annotations[cache.ProjectLastSleepTime]; !ok {
			// This is string eq of time.Time{}
			if lastSleepTimeStr == "0001-01-01 00:00:00 +0000 UTC" {
				lastSleepTime = time.Time{}
			}
		} else {
			lastSleepTime, err = time.Parse(time.RFC3339, lastSleepTimeStr)
			if err != nil {
				glog.Errorf("Force-sleeper: %s", err)
				return
			}
		}
		if !lastSleepTime.IsZero() {
			if time.Since(lastSleepTime) < s.config.ProjectSleepPeriod {
				return
			} else {
				// Wake the project if project has been asleep longer than force-sleep period
				err := s.wakeProject(namespace)
				if err != nil {
					glog.Errorf("Force-sleeper: %s", err)
					return
				}
			}
		}
		glog.V(2).Infof("Force-sleeper: Project( %s )sync complete", namespace)
		return
	} else {
		project, err := s.resourceStore.ProjectList.Get(namespace)
		if err != nil {
			glog.Errorf("Force-sleeper: Error: %s", err)
			return
		}
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
				// TODO: This is not exact.. is there a better way??
				// TODO: Is this Sub ok?
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
				glog.Errorf("Force-sleeper: Error applying project( %s )sleep quota: %s", namespace, err)
				return
			}
			// We want DeadPodsRuntime to reset to 0 here, after pods in project are deleted
			project, err := s.resourceStore.ProjectList.Get(namespace)
			if err != nil {
				glog.Errorf("Force-sleeper: %s", err)
				return
			}
			pCopy := project.DeepCopy()
			delete(pCopy.Annotations, cache.ProjectDeadPodsRuntimeAnnotation)
			_, err = s.resourceStore.KubeClient.CoreV1().Namespaces().Update(pCopy)
			if err != nil {
				glog.Errorf("Force-sleeper: %s", err)
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
	if s.config.DryRun {
		glog.V(2).Infof("Force-sleeper DryRun: Simulating project( %s )sleep in dry run mode", namespace)
	}
	// TODO: ok to do in DryRun?
	// Add force-sleep annotation to project
	err := s.addNamespaceSleepTimeAnnotation(namespace, sleepTime)
	if err != nil {
		return err
	}
	// Do not make any quota, idling, or scaling changes in dry-run mode
	if s.config.DryRun {
		glog.V(0).Infof("Force-sleeper: No resource-quota will be set and project deployments will not be scaled, since force-sleep DryRun=True")
		return nil
	}
	// Add force-sleep resource quota to object
	failed := false
	quotaInterface := s.resourceStore.KubeClient.CoreV1().ResourceQuotas(namespace)
	_, err = quotaInterface.Create(s.projectSleepQuota)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: %s", err)
	}
	err = s.resourceStore.CreateOrUpdateIdler(namespace, false)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: %s", err)
	}
	// If failed, undo whatever has been done so project is not left in half-sleep
	if failed {
		err = s.removeNamespaceSleepTimeAnnotation(namespace)
		if err != nil {
			glog.Errorf("Force-sleeper: Error removing sleepTime annotation in project( %s ): %s", namespace, err)
		}
		err = s.resourceStore.KubeClient.CoreV1().ResourceQuotas(namespace).Delete(cache.ProjectSleepQuotaName, &metav1.DeleteOptions{})
		if err != nil {
			if !kerrors.IsNotFound(err) {
				glog.Errorf("Force-sleeper: Error removing force-sleep quota from project( %s ): %s", namespace, err)
			}
		}
		err = s.resourceStore.IdlersClient.Idlers(namespace).Delete(cache.HibernationIdler, &metav1.DeleteOptions{})
		if err != nil {
			if !kerrors.IsNotFound(err) {
				glog.Errorf("Force-sleeper: Error removing hibernation idler from project( %s ): %s", namespace, err)
			}
		}
		glog.Errorf("Force-sleeper: Error applying sleep for project %s", namespace)
	}
	return nil
}

// Adds a cache.ProjectLastSleepTime annotation to a namespace in the cluster
func (s *Sleeper) addNamespaceSleepTimeAnnotation(namespace string, sleepTime time.Time) error {
	//ns, err := s.resourceStore.ProjectList.Get(namespace)
	nsCopy, err := s.resourceStore.ProjectList.Get(namespace)
	if err != nil {
		return err
	}
	//nsCopy := ns.DeepCopy()
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
	// These should already be deleted, but if not...
	delete(projectCopy.Annotations, cache.ProjectDeadPodsRuntimeAnnotation)
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
	if s.config.DryRun {
		glog.V(2).Infof("Force-sleeper DryRun: Simulating project( %s )wake in dry run mode", namespace)
	}
	failed := false
	var plst time.Time
	plstStr := project.Annotations[cache.ProjectLastSleepTime]
	plst, err = time.Parse(time.RFC3339, plstStr)
	if err != nil {
		return err
	}
	if plst.IsZero() {
		return fmt.Errorf("Force-sleeper: Error: was expecting a cache.ProjectLastSleepTime annotation on project( %s )", namespace)
	}
	if time.Since(plst) > s.config.ProjectSleepPeriod {
		// First remove the force-sleep pod count resource quota from the project
		glog.V(2).Infof("Force-sleeper: Removing sleep quota for project( %s )", namespace)
		if !s.config.DryRun {
			quotaInterface := s.resourceStore.KubeClient.CoreV1().ResourceQuotas(namespace)
			err = quotaInterface.Delete(ProjectSleepQuotaName, &metav1.DeleteOptions{})
			if err != nil {
				if !kerrors.IsNotFound(err) {
					return fmt.Errorf("Force-sleeper: Error removing sleep quota in project( %s ): %s", namespace, err)

				}
				glog.V(2).Infof("Force-sleeper: Sleep quota not found in project( %s ), could not remove", namespace)
			}
		}
		//TODO
		// Remove sleep time annotation, if it exists, even in DRY RUN?
		err = s.removeNamespaceSleepTimeAnnotation(namespace)
		if err != nil {
			failed = true
			glog.Errorf("Force-sleeper: Error removing project( %s )sleep time annotation: %s", namespace, err)
		}
		if !s.config.DryRun {
			glog.V(2).Infof("Force-sleeper: Adding project( %s )TriggerServiceNames for idling", namespace)
			err = s.resourceStore.CreateOrUpdateIdler(namespace, true)
			if err != nil {
				failed = true
				return fmt.Errorf("Force-sleeper: Error: %s", err)
			}

		}
		project, err := s.resourceStore.ProjectList.Get(namespace)
		if err != nil {
			return err
		}
		projectCopy := project.DeepCopy()
		projectCopy.Annotations[cache.ProjectLastSleepTime] = time.Time{}.String()
		_, err = s.resourceStore.KubeClient.CoreV1().Namespaces().Update(projectCopy)
		if err != nil {
			return fmt.Errorf("Force-sleeper: Error: %s", err)
		}
	}
	// if anything failed, remove anything half-done from project
	if failed {
		err = s.removeNamespaceSleepTimeAnnotation(namespace)
		if err != nil {
			glog.Errorf("Force-sleeper: Error removing sleepTime annotation in project( %s ): %s", namespace, err)
		}
		err = s.resourceStore.KubeClient.CoreV1().ResourceQuotas(namespace).Delete(cache.ProjectSleepQuotaName, &metav1.DeleteOptions{})
		if err != nil {
			if !kerrors.IsNotFound(err) {
				glog.Errorf("Force-sleeper: Error removing force-sleep quota from project( %s ): %s", namespace, err)
			}
		}
		err = s.resourceStore.IdlersClient.Idlers(namespace).Delete(cache.HibernationIdler, &metav1.DeleteOptions{})
		if err != nil {
			if !kerrors.IsNotFound(err) {
				glog.Errorf("Force-sleeper: Error removing hibernation idler from project( %s ): %s", namespace, err)
			}
		}
		glog.Errorf("Force-sleeper: Error: Failed to wake all services in project( %s )", namespace)
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
