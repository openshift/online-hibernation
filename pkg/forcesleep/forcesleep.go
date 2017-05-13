package forcesleep

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/openshift/online/force-sleep/pkg/cache"
	"github.com/openshift/online/force-sleep/pkg/idling"

	"github.com/openshift/origin/pkg/cmd/util/clientcmd"

	"github.com/golang/glog"

	kapi "k8s.io/kubernetes/pkg/api"
	kerrors "k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/wait"
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
}

type Sleeper struct {
	factory           *clientcmd.Factory
	config            *SleeperConfig
	resources         *cache.Cache
	projectSleepQuota runtime.Object
	stopChannel       <-chan struct{}
}

func NewSleeper(sc *SleeperConfig, f *clientcmd.Factory, c *cache.Cache) *Sleeper {
	ctrl := &Sleeper{
		config:    sc,
		factory:   f,
		resources: c,
	}
	quota, err := ctrl.createSleepResources()
	if err != nil {
		glog.Fatalf("Force-sleeper: Error creating sleep resources: %s", err)
	}
	ctrl.projectSleepQuota = quota
	return ctrl
}

// Main function for controller
func (s *Sleeper) Run(stopChan <-chan struct{}) {
	s.stopChannel = stopChan

	// Spawn a goroutine to run project sync
	go wait.Until(s.Sync, s.config.SleepSyncPeriod, stopChan)

}

// Spawns goroutines to sync projects
func (s *Sleeper) Sync() {
	glog.V(1).Infof("Force-sleeper: Running project sync\n")
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
		s.SyncProject(namespace)
	}
}

func (s *Sleeper) applyProjectSleep(namespace string, sleepTime, wakeTime time.Time) error {
	proj, err := s.resources.GetProject(namespace)
	if err != nil {
		return err
	}
	projectCopy, err := kapi.Scheme.DeepCopy(proj)
	if err != nil {
		return fmt.Errorf("Force-sleeper: Couldn't copy project from cache: %v", err)
	}
	project := projectCopy.(*cache.ResourceObject)

	glog.V(2).Infof("Force-sleeper: Adding sleep quota for project %s.\n", namespace)
	project.LastSleepTime = sleepTime
	project.IsAsleep = true
	err = s.resources.Indexer.UpdateResourceObject(project)
	if err != nil {
		glog.Errorf("Force-sleeper: Error setting LastSleepTime for project %s: %s\n", namespace, err)
	}
	// Check that resource was updated
	sleepingProj, err := s.resources.GetProject(namespace)
	if err != nil {
		panic("Could not get project sleep state")
	}
	if !sleepingProj.IsAsleep {
		panic("Project is not set to 'IsAsleep' in cache")
	}
	quotaInterface := s.resources.KubeClient.ResourceQuotas(namespace)
	quota := s.projectSleepQuota.(*kapi.ResourceQuota)
	_, err = quotaInterface.Create(quota)
	if err != nil {
		return err
	}

	failed := false

	glog.V(2).Infof("Force-sleeper: Annotating services in project %s.\n", namespace)
	err = idling.AddProjectPreviousScaleAnnotation(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error annotating services in project %s: %s\n", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Scaling DCs in project %s\n", namespace)
	err = idling.ScaleProjectDCs(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error scaling DCs in project %s: %s\n", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Scaling RCs in project %s\n", namespace)
	err = idling.ScaleProjectRCs(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error scaling RCs in project %s: %s\n", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Deleting pods in project %s\n", namespace)
	err = s.deleteProjectPods(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error deleting pods in project %s: %s\n", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Clearing cache for project %s\n", namespace)
	err = s.clearProjectCache(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error clearing cache in project %s: %s\n", namespace, err)
	}
	if failed {
		glog.Errorf("Force-sleeper: Error applying sleep for project %s\n", namespace)
	}
	return nil
}

func (s *Sleeper) clearProjectCache(namespace string) error {
	resources, err := s.resources.Indexer.ByIndex("byNamespace", namespace)
	if err != nil {
		return err
	}
	for _, resource := range resources {
		r := resource.(*cache.ResourceObject)
		if r.Kind != cache.ProjectKind && r.Kind != cache.ServiceKind {
			s.resources.Indexer.DeleteResourceObject(r)
		}
	}
	return nil
}

func (s *Sleeper) deleteProjectPods(namespace string) error {
	// Delete running pods.
	podInterface := s.resources.KubeClient.Pods(namespace)
	podList, err := podInterface.List(kapi.ListOptions{})
	if err != nil {
		return err
	}
	failed := false
	for _, pod := range podList.Items {
		err = podInterface.Delete(pod.ObjectMeta.Name, &kapi.DeleteOptions{})
		if err != nil {
			if kerrors.IsNotFound(err) {
				continue
			} else {
				glog.Errorf("Force-sleeper: Error deleting pods in namespace %s: %s\n", namespace, err)
				failed = true
			}
		}
	}
	if failed {
		return errors.New("Force-sleeper: Failed to delete all project pods")
	}
	return nil
}

func (s *Sleeper) wakeProject(project *cache.ResourceObject) bool {
	nowTime := time.Now()
	namespace := project.Namespace
	if !project.LastSleepTime.IsZero() {
		if time.Since(project.LastSleepTime) > s.config.ProjectSleepPeriod {
			// First remove the force-sleep pod count resource quota from the project
			glog.V(2).Infof("Force-sleeper: Removing sleep quota for project %s.\n", namespace)
			quotaInterface := s.resources.KubeClient.ResourceQuotas(namespace)
			err := quotaInterface.Delete(ProjectSleepQuotaName)
			if err != nil {
				if kerrors.IsNotFound(err) {
					glog.V(2).Infof("Force-sleeper: Error removing sleep quota: %s", err)
				} else {
					glog.Errorf("Force-sleeper: Error removing sleep quota: %s", err)
				}
			}

			projectCopy, err := kapi.Scheme.DeepCopy(project)
			if err != nil {
				glog.Errorf("Force-sleeper: Couldn't copy project from cache: %v", err)
				return false
			}
			project = projectCopy.(*cache.ResourceObject)

			glog.V(2).Infof("Force-sleeper: Adding project( %s )Idled-At annotations.\n", namespace)
			err = idling.AddProjectIdledAtAnnotation(s.resources, namespace, nowTime)
			if err != nil {
				glog.Errorf("Force-sleeper: Error applying project( %s )idled-at service annotations: %s", namespace, err)
			}
			project.LastSleepTime = time.Time{}
			project.IsAsleep = false
			s.resources.Indexer.UpdateResourceObject(project)
			// Check that resource was updated
			awakeProj, err := s.resources.GetProject(namespace)
			if err != nil {
				panic("Could not get project sleep state")
			}
			if awakeProj.IsAsleep {
				panic("Project is set to 'IsAsleep' in cache")
			}
			return true
		}
	}
	return false
}

func (s *Sleeper) memoryQuota(pod *cache.ResourceObject) resource.Quantity {
	// Get project memory quota
	if pod.Terminating {
		return s.config.TermQuota
	} else {
		return s.config.NonTermQuota
	}

}

// Syncs a project and determines if force-sleep is needed
func (s *Sleeper) SyncProject(namespace string) {
	if s.config.Exclude[namespace] {
		return
	}

	glog.V(2).Infof("Force-sleeper: Syncing project: %s\n", namespace)
	project, err := s.resources.GetProject(namespace)
	if err != nil {
		glog.Errorf("Force-sleeper: Error getting project( %s ):", namespace, err)
	}
	//project.Mutex.Lock()
	//defer project.Mutex.Unlock()
	// Iterate through pods to calculate runtimes
	pods, err := s.resources.GetProjectPods(namespace)
	if err != nil {
		glog.Errorf("Force-sleeper: Error getting project( %s )pod resources:", namespace, err)
	}
	termQuotaSecondsConsumed := 0.0
	nonTermQuotaSecondsConsumed := 0.0
	for _, obj := range pods {
		pod := obj.(*cache.ResourceObject)
		if s.PrunePods(pod) {
			continue
		}
		p, err := kapi.Scheme.DeepCopy(pod)
		if err != nil {
			glog.Errorf("Force-sleeper: Couldn't copy pod from cache: %v", err)
			return
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
			sleepQuota := s.projectSleepQuota.(*kapi.ResourceQuota)
			quotaInterface := s.resources.KubeClient.ResourceQuotas(namespace)
			_, err := quotaInterface.Create(sleepQuota)
			if err != nil && !kerrors.IsAlreadyExists(err) {
				glog.Errorf("Force-sleeper: Error creating sleep quota on project %s: %s\n", namespace, err)
				return
			}
			if kerrors.IsAlreadyExists(err) {
				return
			}
			err = s.applyProjectSleep(namespace, project.LastSleepTime, project.LastSleepTime.Add(s.config.ProjectSleepPeriod))
			if err != nil {
				glog.Errorf("Force-sleeper: Error applying project sleep quota: %s\n", err)
			}
			return
		} else {
			if s.wakeProject(project) {
				return
			}
		}
	}
	if quotaSecondsConsumed > s.config.Quota.Seconds() {
		// Project-level sleep
		glog.V(2).Infof("Force-sleeper: Project( %s )over quota! (%+vs/%+vs), applying force-sleep...\n", namespace, quotaSecondsConsumed, s.config.Quota.Seconds())
		err = s.applyProjectSleep(namespace, time.Now(), time.Now().Add(s.config.ProjectSleepPeriod))
		if err != nil {
			glog.Errorf("Force-sleeper: Error applying project sleep quota: %s\n", err)
		}
		return
	}

	// Project sort index:
	s.updateProjectSortIndex(namespace, quotaSecondsConsumed)
	glog.V(2).Infof("Force-sleeper: Project( %s )sync complete.\n", namespace)
}

func (s *Sleeper) scaleDownRC(name, namespace string) error {
	glog.V(3).Infof("Force-sleeper: Scaling RC %s in project( %s )\n", name, namespace)
	thisRC, err := s.resources.KubeClient.ReplicationControllers(namespace).Get(name)
	if err != nil {
		return err
	}
	thisRC.Spec.Replicas = 0
	_, err = s.resources.KubeClient.ReplicationControllers(namespace).Update(thisRC)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sleeper) updateProjectSortIndex(namespace string, quotaSeconds float64) {
	proj, err := s.resources.GetProject(namespace)
	if err != nil {
		glog.Errorf("Force-sleeper: Error getting project resources: %s", err)
	}
	projectCopy, err := kapi.Scheme.DeepCopy(proj)
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
			panic("Object passed to Prune Resource Not a Pod")
		}
	} else {
		return false
	}
}
