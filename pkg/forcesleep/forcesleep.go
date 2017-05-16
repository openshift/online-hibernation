package forcesleep

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/openshift/online/force-sleep/pkg/cache"
	"github.com/openshift/online/force-sleep/pkg/idling"

	oscache "github.com/openshift/origin/pkg/client/cache"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"

	"github.com/golang/glog"

	kapi "k8s.io/kubernetes/pkg/api"
	kerrors "k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/api/resource"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/watch"
)

const (
	PodKind                   = "pods"
	RCKind                    = "replicationcontrollers"
	ServiceKind               = "services"
	ProjectKind               = "project"
	ComputeQuotaName          = "compute-resources"
	ComputeTimeboundQuotaName = "compute-resources-timebound"
	ProjectSleepQuotaName     = "force-sleep"
	OpenShiftDCName           = "openshift.io/deployment-config.name"
)

type SleeperConfig struct {
	Quota              time.Duration
	Period             time.Duration
	SyncPeriod         time.Duration
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
		glog.Fatalf("Error creating sleep resources: %s", err)
	}
	ctrl.projectSleepQuota = quota
	return ctrl
}

// Main function for controller
func (s *Sleeper) Run(stopChan <-chan struct{}) {
	s.stopChannel = stopChan

	// Call to watch for incoming events
	s.WatchForEvents()

	// Spawn a goroutine to run project sync
	go wait.Until(s.Sync, s.config.SyncPeriod, stopChan)
}

// Creates 2 goroutines that listen for events to Pods or RCs
func (s *Sleeper) WatchForEvents() {
	eventQueue := oscache.NewEventQueue(kcache.MetaNamespaceKeyFunc)

	podLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return s.resources.KubeClient.Pods(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return s.resources.KubeClient.Pods(kapi.NamespaceAll).Watch(options)
		},
	}
	kcache.NewReflector(podLW, &kapi.Pod{}, eventQueue, 0).Run()

	rcLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return s.resources.KubeClient.ReplicationControllers(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return s.resources.KubeClient.ReplicationControllers(kapi.NamespaceAll).Watch(options)
		},
	}
	kcache.NewReflector(rcLW, &kapi.ReplicationController{}, eventQueue, 0).Run()

	svcLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return s.resources.KubeClient.Services(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return s.resources.KubeClient.Services(kapi.NamespaceAll).Watch(options)
		},
	}
	kcache.NewReflector(svcLW, &kapi.Service{}, eventQueue, 0).Run()

	go func() {
		for {
			event, res, err := eventQueue.Pop()
			err = s.handleResource(event, res)
			if err != nil {
				glog.Errorf("Error capturing event: %s", err)
			}
		}
	}()
}

// Adds a new runningtime start value to a resource object
func (s *Sleeper) startResource(r *cache.ResourceObject) {
	s.resources.CreateProjectInCache(r.Namespace)
	UID := string(r.UID)
	if !s.resources.ResourceInCache(UID) {
		s.resources.Add(r)
	}

	// Make sure object was successfully created before working on it
	obj, exists, err := s.resources.GetResourceByKey(UID)
	res, err := kapi.Scheme.DeepCopy(obj)
	if err != nil {
		glog.Errorf("couldn't copy resource from cache: %v", err)
		return
	}
	if exists {
		resource := res.(*cache.ResourceObject)
		glog.V(3).Infof("Starting resource: %s\n", resource.Name)
		if !resource.IsStarted() {
			runTime := &cache.RunningTime{
				Start: time.Now(),
			}
			resource.RunningTimes = append(resource.RunningTimes, runTime)
			s.resources.Update(resource)
		}
	} else {
		glog.Errorf("Error starting resource: could not find resource %s\n", UID)
	}
}

// Adds an end time to a resource object
func (s *Sleeper) stopResource(r *cache.ResourceObject) {
	var stopTime time.Time

	s.resources.CreateProjectInCache(r.Namespace)
	resourceTime := r.DeletionTimestamp
	UID := string(r.UID)

	// See if we already have a reference to this resource in cache
	if s.resources.ResourceInCache(UID) {
		// Use the resource's given deletion time, if it exists
		if !resourceTime.IsZero() {
			stopTime = resourceTime
		}
	} else {
		// If resource is not in cache, then ignore Delete event
		return
	}

	// If there is an error with the given deletion time, use "now"
	now := time.Now()
	if stopTime.After(now) || stopTime.IsZero() {
		stopTime = now
	}

	obj, exists, err := s.resources.GetResourceByKey(UID)
	res, err := kapi.Scheme.DeepCopy(obj)
	if err != nil {
		glog.Errorf("couldn't copy resource from cache: %v", err)
		return
	}
	if exists {
		resource := res.(*cache.ResourceObject)
		glog.V(3).Infof("Stopping resource: %s\n", resource.Name)
		runTimeCount := len(resource.RunningTimes)

		if resource.IsStarted() {
			resource.RunningTimes[runTimeCount-1].End = stopTime
			s.resources.Update(resource)
		}
	} else {
		glog.Errorf("Error stopping resource: could not find resource %s\n", UID)
	}
}

// Handles incoming events for resources of any type
func (s *Sleeper) handleResource(eventType watch.EventType, resource interface{}) error {
	switch eventType {
	case watch.Added, watch.Modified:
		switch r := resource.(type) {
		case *kapi.Pod:
			s.handlePodChange(r)
		case *kapi.ReplicationController:
			s.handleRCChange(r)
		case *kapi.Service:
			s.handleServiceChange(r)
		}
	case watch.Deleted:
		glog.V(3).Infof("Received DELETE event\n")
		r := cache.NewResourceFromInterface(resource)
		if r != nil {
			s.stopResource(r)
		}
	}
	return nil
}

// Function for handling Pod ADD/MODIFY events
func (s *Sleeper) handlePodChange(pod *kapi.Pod) {
	glog.V(3).Infof("Received ADD/MODIFY for pod: %s\n", pod.Name)
	resourceObject := cache.NewResourceFromPod(pod)

	// If the pod is running, make sure it's started in cache
	// Otherwise, make sure it's stopped
	switch pod.Status.Phase {
	case kapi.PodRunning:
		s.startResource(resourceObject)
	case kapi.PodSucceeded, kapi.PodFailed, kapi.PodUnknown:
		s.stopResource(resourceObject)
	}
}

// Function for handling RC ADD/MODIFY events
func (s *Sleeper) handleRCChange(rc *kapi.ReplicationController) {
	glog.V(3).Infof("Received ADD/MODIFY for RC: %s\n", rc.Name)
	resourceObject := cache.NewResourceFromRC(rc)

	// If RC has more than 0 active replicas, make sure it's started in cache
	if rc.Status.Replicas > 0 {
		s.startResource(resourceObject)
	} else { // replicas == 0
		s.stopResource(resourceObject)
	}
}

// Function for handling Service ADD/MODIFY events
func (s *Sleeper) handleServiceChange(svc *kapi.Service) {
	glog.V(3).Infof("Received ADD/MODIFY for Service: %s\n", svc.Name)
	r := cache.NewResourceFromService(svc)
	s.resources.CreateProjectInCache(r.Namespace)
	if !s.resources.ResourceInCache(string(r.UID)) {
		s.resources.Add(r)
	} else {
		s.resources.Update(r)
	}
}

// Spawns goroutines to sync projects
func (s *Sleeper) Sync() {
	glog.V(1).Infof("Running project sync\n")
	projects, err := s.resources.ByIndex("ofKind", ProjectKind)
	if err != nil {
		glog.Errorf("Error getting projects for sync: %s", err)
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
		return fmt.Errorf("couldn't copy project from cache: %v", err)
	}
	project := projectCopy.(*cache.ResourceObject)

	glog.V(2).Infof("Adding sleep quota for project %s\n", namespace)
	// TODO: Change this to a deep copy to avoid mutating the indexed object
	project.LastSleepTime = sleepTime
	project.IsAsleep = true
	err = s.resources.Update(project)
	if err != nil {
		glog.Errorf("Error setting LastSleepTime for project %s: %s\n", namespace, err)
	}

	quotaInterface := s.resources.KubeClient.ResourceQuotas(namespace)
	quota := s.projectSleepQuota.(*kapi.ResourceQuota)
	_, err = quotaInterface.Create(quota)
	if err != nil {
		return err
	}

	failed := false

	glog.V(2).Infof("Idling services in project %s\n", namespace)
	err = idling.AddProjectPreviousScaleAnnotation(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Error idling services in project %s: %s\n", namespace, err)
	}

	glog.V(2).Infof("Scaling DCs in project %s\n", namespace)
	err = idling.ScaleProjectDCs(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Error scaling DCs in project %s: %s\n", namespace, err)
	}

	glog.V(2).Infof("Scaling RCs in project %s\n", namespace)
	err = idling.ScaleProjectRCs(s.resources, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Error scaling RCs in project %s: %s\n", namespace, err)
	}

	glog.V(2).Infof("Deleting pods in project %s\n", namespace)
	err = s.deleteProjectPods(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Error deleting pods in project %s: %s\n", namespace, err)
	}

	glog.V(2).Infof("Clearing cache for project %s\n", namespace)
	err = s.clearProjectCache(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Error clearing cache in project %s: %s\n", namespace, err)
	}
	if failed {
		return fmt.Errorf("Error applying project sleep\n")
	}
	return nil
}

func (s *Sleeper) clearProjectCache(namespace string) error {
	resources, err := s.resources.ByIndex("byNamespace", namespace)
	if err != nil {
		return err
	}
	for _, resource := range resources {
		r := resource.(*cache.ResourceObject)
		if r.Kind != ProjectKind && r.Kind != ServiceKind {
			s.resources.Delete(r)
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
				glog.Errorf("Error deleting pods in namespace %s: %s\n", namespace, err)
				failed = true
			}
		}
	}
	if failed {
		return errors.New("Failed to delete all project pods")
	}
	return nil
}

func (s *Sleeper) wakeProject(project *cache.ResourceObject) bool {
	nowTime := time.Now()
	namespace := project.Namespace
	if !project.LastSleepTime.IsZero() {
		if time.Since(project.LastSleepTime) > s.config.ProjectSleepPeriod {
			// First remove the force-sleep pod count resource quota from the project
			glog.V(2).Infof("Removing sleep quota for project %s\n", namespace)
			quotaInterface := s.resources.KubeClient.ResourceQuotas(namespace)
			err := quotaInterface.Delete(ProjectSleepQuotaName)
			if err != nil {
				if kerrors.IsNotFound(err) {
					glog.V(0).Infof("Error removing sleep quota: %s", err)
				} else {
					glog.Errorf("Error removing sleep quota: %s", err)
				}
			}
			projectCopy, err := kapi.Scheme.DeepCopy(project)
			if err != nil {
				glog.Errorf("couldn't copy project from cache: %v", err)
				return false
			}
			project = projectCopy.(*cache.ResourceObject)

			project.LastSleepTime = time.Time{}
			project.IsAsleep = false
			s.resources.Update(project)

			glog.V(2).Infof("Adding project Idled-At annotations")
			err = idling.AddProjectIdledAtAnnotation(s.resources, namespace, nowTime)
			if err != nil {
				glog.Errorf("Error applying service unidling: %s", err)
			}
			return true
		}
	}
	return false
}

func (s *Sleeper) memoryQuota(namespace string, pod *cache.ResourceObject) resource.Quantity {
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

	glog.V(2).Infof("Syncing project: %s\n", namespace)
	project, err := s.resources.GetProject(namespace)
	if err != nil {
		glog.Errorf("Error getting project (%s):", namespace, err)
		return
	}

	// Iterate through pods to calculate runtimes
	pods, err := s.resources.GetProjectPods(namespace)
	if err != nil {
		glog.Errorf("Error getting project (%s) pod resources:", namespace, err)
	}
	termQuotaSecondsConsumed := 0.0
	nonTermQuotaSecondsConsumed := 0.0
	for _, obj := range pods {
		pod := obj.(*cache.ResourceObject)
		if s.PruneResource(pod) {
			continue
		}

		p, err := kapi.Scheme.DeepCopy(pod)
		if err != nil {
			glog.Errorf("couldn't copy pod from cache: %v", err)
			return
		}
		newPod := p.(*cache.ResourceObject)
		totalRuntime, changed := newPod.GetResourceRuntime(s.config.Period)
		if changed {
			s.resources.Update(newPod)
		}
		seconds := float64(totalRuntime.Seconds())
		memoryLimit := s.memoryQuota(namespace, pod)
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
				glog.Errorf("Error creating sleep quota on project %s: %s\n", namespace, err)
				return
			}
			if kerrors.IsAlreadyExists(err) {
				return
			}
			err = s.applyProjectSleep(namespace, project.LastSleepTime, project.LastSleepTime.Add(s.config.ProjectSleepPeriod))
			if err != nil {
				glog.Errorf("Error applying project sleep quota: %s\n", err)
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
		glog.V(2).Infof("Project %s over quota! (%+vs/%+vs)\n", namespace, quotaSecondsConsumed, s.config.Quota.Seconds())
		err = s.applyProjectSleep(namespace, time.Now(), time.Now().Add(s.config.ProjectSleepPeriod))
		if err != nil {
			glog.Errorf("Error applying project sleep quota: %s\n", err)
		}
		return
	}

	// Project sort index:
	s.updateProjectSortIndex(namespace, quotaSecondsConsumed)
}

func (s *Sleeper) scaleDownRC(name, namespace string) error {
	glog.V(3).Infof("Scaling RC %s in project (%s)\n", name, namespace)
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
		glog.Errorf("Error getting project resources: %s", err)
	}

	projectCopy, err := kapi.Scheme.DeepCopy(proj)
	if err != nil {
		glog.Errorf("couldn't copy project from cache: %v", err)
		return
	}
	project := projectCopy.(*cache.ResourceObject)

	// Projects closer to force-sleep will have a lower index value
	sortIndex := -1 * quotaSeconds
	project.ProjectSortIndex = sortIndex
	s.resources.Update(project)
}

// Check to clear cached resources whose runtimes are outside of the period, and thus irrelevant
func (s *Sleeper) PruneResource(resource *cache.ResourceObject) bool {
	count := len(resource.RunningTimes)
	if count < 1 {
		return true
	}
	if resource.IsStarted() {
		return false
	}
	lastTime := resource.RunningTimes[count-1]
	if time.Since(lastTime.End) > s.config.Period {
		s.resources.Delete(resource)
		return true
	} else {
		return false
	}
}
