package forcesleep

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"
	"github.com/openshift/online-hibernation/pkg/idling"

	"github.com/golang/glog"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientv1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	ComputeQuotaName          = "compute-resources"
	ComputeTimeboundQuotaName = "compute-resources-timebound"
	ProjectSleepQuotaName     = "force-sleep"
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
	QuotaClient        clientv1.ResourceQuotasGetter
}

type Sleeper struct {
	config            *SleeperConfig
	resources         *cache.Cache
	projectSleepQuota *corev1.ResourceQuota
	stopChan          <-chan struct{}
}

func NewSleeper(sc *SleeperConfig, c *cache.Cache) *Sleeper {
	ctrl := &Sleeper{
		config:    sc,
		resources: c,
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
	projects, err := s.resources.Indexer.ByIndex("ofKind", cache.ProjectKind)
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
		namespaces <- namespace.(*cache.ResourceObject).Name
	}
	close(namespaces)
}

func (s *Sleeper) startWorker(namespaces <-chan string) {
	for namespace := range namespaces {
		s.syncProject(namespace)
	}
}

func (s *Sleeper) applyProjectSleep(namespace string, sleepTime, wakeTime time.Time) error {
	if s.config.DryRun {
		glog.V(2).Infof("Force-sleeper DryRun: Simulating project( %s )sleep in dry run mode", namespace)
	}
	proj, err := s.resources.GetProject(namespace)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	projectCopy, err := cache.Scheme.DeepCopy(proj)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	project := projectCopy.(*cache.ResourceObject)
	project.LastSleepTime = sleepTime
	project.IsAsleep = true
	err = s.resources.Indexer.UpdateResourceObject(project)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	// Check that resource was updated
	sleepingProj, err := s.resources.GetProject(namespace)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	if !sleepingProj.IsAsleep {
		return fmt.Errorf("Force-sleeper: Project( %s )is not set to 'IsAsleep' in cache but should be.", namespace)
	}
	// Do not make any quota, idling, or scaling changes in dry-run mode
	if s.config.DryRun {
		return nil
	}
	// Add force-sleep annotation to project
	err = s.addNamespaceSleepTimeAnnotation(namespace, sleepTime)
	if err != nil {
		if !kerrors.IsAlreadyExists(err) {
			return fmt.Errorf("Force-sleeper: %s", err)
		}
		glog.V(2).Infof("Force-sleeper: Couldn't add project( %s )sleep time annotation: %s", namespace, err)
	}

	// Add force-sleep resource quota to object
	_, err = s.config.QuotaClient.ResourceQuotas(namespace).Create(s.projectSleepQuota)
	if err != nil {
		return fmt.Errorf("force-sleeper: %s", err)
	}

	failed := false

	glog.V(2).Infof("Force-sleeper: Annotating services in project( %s )", namespace)
	err = idling.AddProjectPreviousScaleAnnotation(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: %s", err)
	}
	glog.V(2).Infof("Force-sleeper: Scaling DCs in project( %s )", namespace)
	err = idling.ScaleProjectDCs(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error scaling DCs in project( %s ): %s", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Scaling RCs in project( %s )", namespace)
	err = idling.ScaleProjectRCs(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error scaling RCs in project( %s ): %s", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Scaling ReplicaSets in project( %s )", namespace)
	err = idling.ScaleProjectRSs(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error scaling ReplicaSets in project( %s ): %s", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Scaling StatefulSets in project( %s )", namespace)
	err = idling.ScaleProjectSSs(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error scaling StatefulSets in project( %s ): %s", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Scaling Deploymentss in project( %s )", namespace)
	err = idling.ScaleProjectDeployments(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error scaling Deployments in project( %s ): %s", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Deleting pods in project( %s )", namespace)
	err = idling.DeleteProjectPods(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error deleting pods in project( %s ): %s", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Clearing cache for project( %s )", namespace)
	err = s.clearProjectCache(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error clearing cache in project( %s ): %s", namespace, err)
	}
	if failed {
		glog.Errorf("Force-sleeper: Error applying sleep for project %s", namespace)
	}
	return nil
}

// Adds a cache.LastSleepTimeAnnotation to a namespace in the cluster
func (s *Sleeper) addNamespaceSleepTimeAnnotation(name string, sleepTime time.Time) error {
	ns, err := s.resources.KubeClient.CoreV1().Namespaces().Get(name, metav1.GetOptions{})
	nsCopy, err := cache.Scheme.DeepCopy(ns)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	project := nsCopy.(*corev1.Namespace)
	if project.Annotations == nil {
		project.Annotations = make(map[string]string)
	}
	project.Annotations[cache.LastSleepTimeAnnotation] = sleepTime.String()
	_, err = s.resources.KubeClient.CoreV1().Namespaces().Update(project)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	return nil
}

// Removes the cache.LastSleepTimeAnnotation to a namespace in the cluster
func (s *Sleeper) removeNamespaceSleepTimeAnnotation(name string) error {
	ns, err := s.resources.KubeClient.CoreV1().Namespaces().Get(name, metav1.GetOptions{})
	nsCopy, err := cache.Scheme.DeepCopy(ns)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	project := nsCopy.(*corev1.Namespace)
	if project.Annotations == nil {
		// Sleep time annotation already doesn't exist
		return nil
	}

	delete(project.Annotations, cache.LastSleepTimeAnnotation)
	_, err = s.resources.KubeClient.CoreV1().Namespaces().Update(project)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	return nil
}

func (s *Sleeper) clearProjectCache(namespace string) error {
	resources, err := s.resources.Indexer.ByIndex("byNamespace", namespace)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	for _, resource := range resources {
		r := resource.(*cache.ResourceObject)
		if r.Kind != cache.ProjectKind && r.Kind != cache.ServiceKind {
			s.resources.Indexer.DeleteResourceObject(r)
		}
	}
	return nil
}

// This function checks if a project needs to wake up and if so, takes the actions to do that
func (s *Sleeper) wakeProject(project *cache.ResourceObject) error {
	namespace := project.Namespace
	if s.config.DryRun {
		glog.V(2).Infof("Force-sleeper DryRun: Simulating project( %s )wake in dry run mode", namespace)
	}
	failed := false
	nowTime := time.Now()
	if time.Since(project.LastSleepTime) > s.config.ProjectSleepPeriod {
		// First remove the force-sleep pod count resource quota from the project
		glog.V(2).Infof("Force-sleeper: Removing sleep quota for project( %s )", namespace)
		if !s.config.DryRun {
			quotaInterface := s.resources.KubeClient.CoreV1().ResourceQuotas(namespace)
			err := quotaInterface.Delete(ProjectSleepQuotaName, &metav1.DeleteOptions{})
			if err != nil {
				if !kerrors.IsNotFound(err) {
					return fmt.Errorf("Force-sleeper: Error removing sleep quota in project( %s ): %s", namespace, err)

				}
				glog.V(2).Infof("Force-sleeper: Sleep quota not found in project( %s ), could not remove", namespace)
			}
			// Remove sleep time annotation, if it exists
			err = s.removeNamespaceSleepTimeAnnotation(namespace)
			if err != nil {
				failed = true
				glog.Errorf("Force-sleeper: Error removing project( %s )sleep time annotation: %s", namespace, err)
			}
			glog.V(2).Infof("Force-sleeper: Adding project( %s )Idled-At annotations", namespace)
			err = idling.AddProjectIdledAtAnnotation(s.resources, namespace, nowTime, s.config.ProjectSleepPeriod)
			if err != nil {
				failed = true
				glog.Errorf("Force-sleeper: Error applying project idled-at service annotations: %s", err)
			}
		} else {
			err := s.SetDryRunResourceRuntimes(namespace)
			if err != nil {
				return fmt.Errorf("Force-sleeper DryRun: Error simulating force-sleep wakeup in project: %v", err)
			}
		}

		projectCopy, err := cache.Scheme.DeepCopy(project)
		if err != nil {
			return fmt.Errorf("Force-sleeper: %s", err)
		}
		project = projectCopy.(*cache.ResourceObject)
		project.LastSleepTime = time.Time{}
		project.IsAsleep = false
		s.resources.Indexer.UpdateResourceObject(project)
		// Check that resource was updated
		awakeProj, err := s.resources.GetProject(namespace)
		if err != nil {
			return fmt.Errorf("Force-sleeper: %s", err)
		}
		if awakeProj.IsAsleep {
			return fmt.Errorf("Project( %s )is set to 'IsAsleep' in cache but should not be.", namespace)
		}
		if failed {
			glog.Errorf("Force-sleeper: Failed to wake all services in project( %s )", namespace)
		}
		return nil
	}
	return nil
}

func (s *Sleeper) memoryQuota(pod *cache.ResourceObject) resource.Quantity {
	// Get project memory quota
	if pod.Terminating {
		return s.config.TermQuota
	} else {
		return s.config.NonTermQuota
	}

}

// Modify all ResourceObjects' cached runtimes when removing force-sleep
// when running with Dry Run mode enabled, to better simulate real run times
// (since in Dry Run mode, no resources are actually scaled or deleted with force-sleep)
func (s *Sleeper) SetDryRunResourceRuntimes(namespace string) error {
	glog.V(2).Infof("Force-sleeper DryRun: Simulating resource runtimes for namespace( %s ).", namespace)
	resources, err := s.resources.Indexer.ByIndex("byNamespace", namespace)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	for _, resource := range resources {
		r := resource.(*cache.ResourceObject)
		copy, err := cache.Scheme.DeepCopy(r)
		if err != nil {
			return fmt.Errorf("Force-sleeper: %s", err)
		}
		resource := copy.(*cache.ResourceObject)

		// If a resource is currently running, remove all its previous runtimes (simulating
		// applied force-sleep) and set its most recent start time to now (simulating wakeup)
		// Otherwise, just delete all previous start times
		newRunningTimes := make([]*cache.RunningTime, 0)
		if resource.IsStarted() {
			newRunningTime := &cache.RunningTime{
				Start: time.Now(),
			}
			newRunningTimes = append(newRunningTimes, newRunningTime)
		}
		resource.RunningTimes = newRunningTimes
		s.resources.Indexer.UpdateResourceObject(resource)
	}
	return nil
}

// Syncs a project and determines if force-sleep is needed
func (s *Sleeper) syncProject(namespace string) error {
	if s.config.Exclude[namespace] {
		return nil
	}

	glog.V(2).Infof("Force-sleeper: Syncing project( %s )", namespace)
	project, err := s.resources.GetProject(namespace)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}

	// Iterate through pods to calculate runtimes
	pods, err := s.resources.GetProjectPods(namespace)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	termQuotaSecondsConsumed := 0.0
	nonTermQuotaSecondsConsumed := 0.0
	for _, obj := range pods {
		pod := obj.(*cache.ResourceObject)
		if s.PrunePods(pod) {
			continue
		}
		p, err := cache.Scheme.DeepCopy(pod)
		if err != nil {
			return fmt.Errorf("Force-sleeper: %s", err)
		}
		newPod := p.(*cache.ResourceObject)
		totalRuntime, changed := newPod.GetResourceRuntime(s.config.Period)
		if changed {
			s.resources.Indexer.UpdateResourceObject(newPod)
		}
		seconds := float64(totalRuntime.Seconds())
		memoryLimit := s.memoryQuota(pod)
		quotaSeconds := getQuotaSeconds(seconds, pod.MemoryRequested, memoryLimit)
		if pod.Terminating {
			termQuotaSecondsConsumed += quotaSeconds
		} else {
			nonTermQuotaSecondsConsumed += quotaSeconds
		}
	}
	quotaSecondsConsumed := math.Max(termQuotaSecondsConsumed, nonTermQuotaSecondsConsumed)

	//Check if quota doesn't exist and should
	if !project.LastSleepTime.IsZero() {
		if time.Since(project.LastSleepTime) < s.config.ProjectSleepPeriod {
			err = s.applyProjectSleep(namespace, project.LastSleepTime, project.LastSleepTime.Add(s.config.ProjectSleepPeriod))
			if err != nil {
				return fmt.Errorf("Force-sleeper: %s", err)
			}
			return nil
		} else {
			// Wake the project if project has been asleep longer than force-sleep period
			err := s.wakeProject(project)
			if err != nil {
				return fmt.Errorf("Force-sleeper: %s", err)
			}
			return nil
		}
	}
	if quotaSecondsConsumed > s.config.Quota.Seconds() {
		// Project-level sleep
		glog.V(2).Infof("Force-sleeper: Project( %s )over quota! (%+vs/%+vs), applying force-sleep...", namespace, quotaSecondsConsumed, s.config.Quota.Seconds())
		err = s.applyProjectSleep(namespace, time.Now(), time.Now().Add(s.config.ProjectSleepPeriod))
		if err != nil {
			return fmt.Errorf("Force-sleeper: Error applying project( %s )sleep quota: %s", namespace, err)
		}
		return nil
	}

	// Project sort index:
	s.updateProjectSortIndex(namespace, quotaSecondsConsumed)
	glog.V(2).Infof("Force-sleeper: Project( %s )sync complete", namespace)
	return nil
}

func (s *Sleeper) updateProjectSortIndex(namespace string, quotaSeconds float64) {
	proj, err := s.resources.GetProject(namespace)
	if err != nil {
		glog.Errorf("Force-sleeper: Error getting project resources: %s", err)
	}
	projectCopy, err := cache.Scheme.DeepCopy(proj)
	if err != nil {
		glog.Errorf("Force-sleeper: Couldn't copy project from cache: %v", err)
		return
	}
	project := projectCopy.(*cache.ResourceObject)

	// Projects closer to force-sleep will have a lower index value
	sortIndex := -1 * quotaSeconds
	project.ProjectSortIndex = sortIndex
	s.resources.Indexer.UpdateResourceObject(project)
}

// Check to clear cached resources whose runtimes are outside of the period, and thus irrelevant
func (s *Sleeper) PrunePods(resource *cache.ResourceObject) bool {
	count := len(resource.RunningTimes)
	if count < 1 {
		return true
	}
	if resource.IsStarted() {
		return false
	}
	lastTime := resource.RunningTimes[count-1]
	if time.Since(lastTime.End) > s.config.Period {
		switch resource.Kind {
		case cache.PodKind:
			s.resources.Indexer.DeleteResourceObject(resource)
			return true
		default:
			glog.Errorf("Object passed to Prune Resource Not a Pod")
		}
	}
	return false
}
