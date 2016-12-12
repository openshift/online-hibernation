package forcesleep

import (
	"math"
	"sort"
	"sync"
	"time"

	osclient "github.com/openshift/origin/pkg/client"
	"github.com/openshift/origin/pkg/client/cache"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"

	"github.com/golang/glog"
	"github.com/spf13/pflag"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/api/resource"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/kubectl"
	ctlresource "k8s.io/kubernetes/pkg/kubectl/resource"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/watch"
)

// Custom sorting for prioritizing projects in force-sleep
type Projects []interface{}

func (p Projects) Len() int {
	return len(p)
}
func (p Projects) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p Projects) Less(i, j int) bool {
	p1 := p[i].(*ResourceObject)
	p2 := p[j].(*ResourceObject)
	return p1.ProjectSortIndex < p2.ProjectSortIndex
}

const (
	PodKind                   = "pods"
	RCKind                    = "replicationcontrollers"
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
	kubeClient        kclient.Interface
	osClient          osclient.Interface
	config            *SleeperConfig
	resources         kcache.Indexer
	projectSleepQuota runtime.Object
	stopChannel       <-chan struct{}
}

type ResourceObject struct {
	Mutex            sync.RWMutex
	UID              types.UID
	Name             string
	Namespace        string
	Kind             string
	Terminating      bool
	ResourceVersion  string
	RunningTimes     []*RunningTime
	MemoryRequested  resource.Quantity
	DeploymentConfig string
	LastSleepTime    time.Time
	ProjectSortIndex float64
}

type RunningTime struct {
	Start time.Time
	End   time.Time
}

type watchListItem struct {
	objType   runtime.Object
	watchFunc func(options kapi.ListOptions) (watch.Interface, error)
}

func newResourceFromPod(pod *kapi.Pod) *ResourceObject {
	terminating := false
	if (pod.Spec.RestartPolicy != kapi.RestartPolicyAlways) && (pod.Spec.ActiveDeadlineSeconds != nil) {
		terminating = true
	}
	return &ResourceObject{
		UID:             pod.ObjectMeta.UID,
		Name:            pod.ObjectMeta.Name,
		Namespace:       pod.ObjectMeta.Namespace,
		Kind:            PodKind,
		Terminating:     terminating,
		ResourceVersion: pod.ObjectMeta.ResourceVersion,
		MemoryRequested: pod.Spec.Containers[0].Resources.Requests["memory"],
		RunningTimes:    make([]*RunningTime, 0),
	}
}

func newResourceFromRC(rc *kapi.ReplicationController) *ResourceObject {
	return &ResourceObject{
		UID:              rc.ObjectMeta.UID,
		Name:             rc.ObjectMeta.Name,
		Namespace:        rc.ObjectMeta.Namespace,
		Kind:             RCKind,
		ResourceVersion:  rc.ObjectMeta.ResourceVersion,
		DeploymentConfig: rc.ObjectMeta.Annotations[OpenShiftDCName],
		RunningTimes:     make([]*RunningTime, 0),
	}
}

func newResourceFromProject(namespace string) *ResourceObject {
	return &ResourceObject{
		UID:              types.UID(namespace),
		Name:             namespace,
		Namespace:        namespace,
		Kind:             ProjectKind,
		LastSleepTime:    time.Time{},
		ProjectSortIndex: 0.0,
	}
}

func (s *Sleeper) createProjectInCache(namespace string) {
	// Make sure we have a tracking resource for the project to monitor last sleep time
	proj, err := s.resources.ByIndex("getProject", namespace)
	if err != nil {
		glog.Errorf("Error checking for project (%s) resources: %s", namespace, err)
	}
	if len(proj) == 0 {
		newResource := newResourceFromProject(namespace)
		s.resources.Add(newResource)
	}
}

func NewSleeper(osClient osclient.Interface, kubeClient kclient.Interface, sc *SleeperConfig) *Sleeper {
	ctrl := &Sleeper{
		osClient:   osClient,
		kubeClient: kubeClient,
		config:     sc,
		resources: kcache.NewIndexer(resourceKey, kcache.Indexers{
			"byNamespace":        indexResourceByNamespace,
			"byNamespaceAndKind": indexResourceByNamespaceAndKind,
			"byFullName":         indexResourceByFullName,
			"getProject":         getProjectResource,
			"ofKind":             getAllResourcesOfKind,
			"rcByDC":             getRCByDC,
		}),
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
	eventQueue := cache.NewEventQueue(kcache.MetaNamespaceKeyFunc)

	podLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return s.kubeClient.Pods(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return s.kubeClient.Pods(kapi.NamespaceAll).Watch(options)
		},
	}
	kcache.NewReflector(podLW, &kapi.Pod{}, eventQueue, 0).Run()

	rcLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return s.kubeClient.ReplicationControllers(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return s.kubeClient.ReplicationControllers(kapi.NamespaceAll).Watch(options)
		},
	}
	kcache.NewReflector(rcLW, &kapi.ReplicationController{}, eventQueue, 0).Run()

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

// Checks if resource exists in project cache
func (s *Sleeper) resourceInCache(UID string) bool {
	_, exists, err := s.resources.GetByKey(UID)
	if err != nil {
		return false
	}
	return exists
}

// Adds a new runningtime start value to a resource object
func (s *Sleeper) startResource(UID string) {
	obj, exists, err := s.resources.GetByKey(UID)
	if err != nil {
		glog.Errorf("Error starting resource by UID: %s", err)
		return
	}
	if exists {
		resource := obj.(*ResourceObject)
		glog.V(2).Infof("Starting resource: %s\n", resource.Name)
		if resource.isStopped() {
			runTime := &RunningTime{
				Start: time.Now(),
			}
			resource.RunningTimes = append(resource.RunningTimes, runTime)
			s.resources.Update(resource)
		}
	} else {
		glog.V(2).Infof("Error starting resource: could not find resource %s\n", UID)
	}
}

// Adds an end time to a resource object
func (s *Sleeper) stopResource(UID string, resource interface{}) {
	var stopTime time.Time
	switch r := resource.(type) {
	case *kapi.Pod:
		if s.resourceInCache(UID) {
			if !r.ObjectMeta.DeletionTimestamp.IsZero() {
				stopTime = r.ObjectMeta.DeletionTimestamp.Time
			}
		}

	case *kapi.ReplicationController:
		if s.resourceInCache(UID) {
			if !r.ObjectMeta.DeletionTimestamp.IsZero() {
				stopTime = r.ObjectMeta.DeletionTimestamp.Time
			}
		}
	}

	now := time.Now()
	if stopTime.After(now) || stopTime.IsZero() {
		stopTime = now
	}

	obj, exists, err := s.resources.GetByKey(UID)
	if err != nil {
		glog.Errorf("Error stopping resource by UID: %s", err)
		return
	}
	if exists {
		resource := obj.(*ResourceObject)
		glog.V(2).Infof("Stopping resource: %s\n", resource.Name)
		runTimeCount := len(resource.RunningTimes)

		if resource.isStarted() {
			resource.RunningTimes[runTimeCount-1].End = stopTime
			s.resources.Update(resource)
		}
	} else {
		glog.V(2).Infof("Error stopping resource: could not find resource %s\n", UID)
	}
}

func (r *ResourceObject) isStopped() bool {
	runtimes := len(r.RunningTimes)
	if runtimes == 0 {
		return true
	} else {
		return !(r.RunningTimes[runtimes-1].End.IsZero())
	}

}

func (r *ResourceObject) isStarted() bool {
	runtimes := len(r.RunningTimes)
	if runtimes == 0 {
		return false
	} else {
		return r.RunningTimes[runtimes-1].End.IsZero()
	}
}

func (s *Sleeper) handleResource(eventType watch.EventType, resource interface{}) error {
	var UID string
	switch eventType {
	case watch.Added:
	case watch.Modified:
		switch r := resource.(type) {
		case *kapi.Pod:
			s.createProjectInCache(r.ObjectMeta.Namespace)
			UID = string(r.ObjectMeta.UID)

			glog.V(2).Infof("Received ADD/MODIFY for pod: %s\n", r.Name)
			switch r.Status.Phase {
			case kapi.PodRunning:
				if !s.resourceInCache(UID) {
					newResource := newResourceFromPod(r)
					s.resources.Add(newResource)
				}
				s.startResource(UID)
			case kapi.PodSucceeded:
			case kapi.PodFailed:
			case kapi.PodUnknown:
				s.stopResource(UID, resource)
			}

		case *kapi.ReplicationController:
			s.createProjectInCache(r.ObjectMeta.Namespace)
			UID = string(r.ObjectMeta.UID)

			glog.V(2).Infof("Received ADD/MODIFY for RC: %s\n", r.Name)
			if r.Status.Replicas > 0 {
				if !s.resourceInCache(UID) {
					newResource := newResourceFromRC(r)
					s.resources.Add(newResource)
				}
				s.startResource(UID)
			} else { // replicas == 0
				s.stopResource(UID, resource)
			}

		}
	case watch.Deleted:
		glog.V(2).Infof("Received DELETE event\n")
		s.stopResource(UID, resource)
	}
	return nil
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
		namespaces <- namespace.(*ResourceObject).Name
	}
	close(namespaces)
}

func (s *Sleeper) startWorker(namespaces <-chan string) {
	for namespace := range namespaces {
		s.SyncProject(namespace)
	}
}

func (s *Sleeper) createSleepResources() (runtime.Object, error) {
	quotaGenerator := &kubectl.ResourceQuotaGeneratorV1{
		Name: ProjectSleepQuotaName,
		Hard: "pods=0",
	}
	obj, err := quotaGenerator.StructuredGenerate()
	if err != nil {
		return nil, err
	}
	f := clientcmd.New(pflag.NewFlagSet("empty", pflag.ContinueOnError))

	mapper, typer := f.Object(false)
	resourceMapper := &ctlresource.Mapper{
		ObjectTyper:  typer,
		RESTMapper:   mapper,
		ClientMapper: ctlresource.ClientMapperFunc(f.ClientForMapping),
	}
	info, err := resourceMapper.InfoForObject(obj, nil)
	if err != nil {
		return nil, err
	}
	if err := kubectl.UpdateApplyAnnotation(info, f.JSONEncoder()); err != nil {
		return nil, err
	}
	return info.Object, nil
}

func (s *Sleeper) applyProjectSleep(namespace string, sleepTime, wakeTime time.Time) error {
	obj, err := s.resources.ByIndex("getProject", namespace)
	if err != nil {
		return err
	}
	if len(obj) == 1 {
		glog.V(2).Infof("Adding sleep quota for project %s\n", namespace)
		project := obj[0].(*ResourceObject)
		// TODO: Change this to a deep copy to avoid mutating the indexed object
		project.LastSleepTime = sleepTime
		err = s.resources.Update(project)
		if err != nil {
			glog.V(2).Infof("Error setting LastSleepTime for project %s: %s\n", namespace, err)
		}

		quotaInterface := s.kubeClient.ResourceQuotas(namespace)
		quota := s.projectSleepQuota.(*kapi.ResourceQuota)
		_, err := quotaInterface.Create(quota)
		if err != nil {
			return err
		}

	}

	glog.V(2).Infof("Scaling RCs in project %s\n", namespace)
	err = s.scaleProjectRCs(namespace)
	if err != nil {
		return err
	}

	glog.V(2).Infof("Scaling DCs in project %s\n", namespace)
	err = s.scaleProjectDCs(namespace)
	if err != nil {
		return err
	}

	glog.V(2).Infof("Deleting pods in project %s\n", namespace)
	err = s.deleteProjectPods(namespace)
	if err != nil {
		return err
	}

	glog.V(2).Infof("Clearing cache for project %s\n", namespace)
	err = s.clearProjectCache(namespace)
	if err != nil {
		return err
	}

	return nil
}

func (s *Sleeper) clearProjectCache(namespace string) error {
	resources, err := s.resources.ByIndex("byNamespace", namespace)
	if err != nil {
		return err
	}
	for _, resource := range resources {
		r := resource.(*ResourceObject)
		if r.Kind != ProjectKind {
			s.resources.Delete(r)
		}
	}
	return nil
}

func (s *Sleeper) scaleProjectDCs(namespace string) error {
	dcInterface := s.osClient.DeploymentConfigs(namespace)
	dcList, err := dcInterface.List(kapi.ListOptions{})
	if err != nil {
		return err
	}
	for _, thisDC := range dcList.Items {
		thisDC.Spec.Replicas = 0
		_, err = dcInterface.Update(&thisDC)
		if err != nil {
			glog.Errorf("Error scaling DC in namespace %s: %s\n", namespace, err)
		}

	}
	return nil

}

func (s *Sleeper) scaleProjectRCs(namespace string) error {
	// Scale RCs to 0
	rcInterface := s.kubeClient.ReplicationControllers(namespace)
	rcList, err := rcInterface.List(kapi.ListOptions{})
	if err != nil {
		return err
	}
	for _, thisRC := range rcList.Items {
		thisRC.Spec.Replicas = 0
		_, err = rcInterface.Update(&thisRC)
		if err != nil {
			glog.Errorf("Error scaling RC in namespace %s: %s\n", namespace, err)
		}
	}
	return nil
}

func (s *Sleeper) deleteProjectPods(namespace string) error {
	// Delete running pods.
	podInterface := s.kubeClient.Pods(namespace)
	podList, err := podInterface.List(kapi.ListOptions{})
	if err != nil {
		return err
	}
	for _, pod := range podList.Items {
		err = podInterface.Delete(pod.ObjectMeta.Name, &kapi.DeleteOptions{})
		if err != nil {
			glog.Errorf("Error deleting pods in namespace %s: %s\n", namespace, err)
		}
	}
	return nil
}

func (s *Sleeper) wakeProject(project *ResourceObject) bool {
	namespace := project.Namespace
	if !project.LastSleepTime.IsZero() {
		if time.Since(project.LastSleepTime) > s.config.ProjectSleepPeriod {
			glog.V(2).Infof("Removing sleep quota for project %s\n", namespace)
			quotaInterface := s.kubeClient.ResourceQuotas(namespace)
			err := quotaInterface.Delete(ProjectSleepQuotaName)
			if errors.IsNotFound(err) {
				glog.V(0).Infof("Error removing sleep quota: %s", err)
			} else if err != nil {
				glog.Errorf("Error removing sleep quota: %s", err)
			}
			project.LastSleepTime = time.Time{}
			s.resources.Update(project)
			return true
		}
	}
	return false
}

func (s *Sleeper) memoryQuota(namespace string, pod *ResourceObject) resource.Quantity {
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
	projObj, err := s.resources.ByIndex("getProject", namespace)
	if err != nil {
		glog.Errorf("Error getting project resources: %s", err)
		return
	}
	project := projObj[0].(*ResourceObject)

	// Iterate through pods to calculate runtimes
	namespaceAndKind := namespace + "/" + PodKind
	pods, err := s.resources.ByIndex("byNamespaceAndKind", namespaceAndKind)
	if err != nil {
		glog.Errorf("Error getting project (%s) pod resources:", namespace, err)
	}
	termQuotaSecondsConsumed := 0.0
	nonTermQuotaSecondsConsumed := 0.0
	for _, obj := range pods {
		pod := obj.(*ResourceObject)
		totalRuntime := pod.GetResourceRuntime(s.config.Period)
		if s.PruneResource(pod) {
			continue
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
			quotaInterface := s.kubeClient.ResourceQuotas(namespace)
			_, err := quotaInterface.Create(sleepQuota)
			if err != nil && !errors.IsAlreadyExists(err) {
				glog.V(2).Infof("Error creating sleep quota on project %s: %s\n", namespace, err)
				return
			}
			if errors.IsAlreadyExists(err) {
				return
			}
			err = s.applyProjectSleep(namespace, project.LastSleepTime, project.LastSleepTime.Add(s.config.ProjectSleepPeriod))
			if err != nil {
				glog.V(2).Infof("Error applying project sleep quota: %s\n", err)
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
			glog.V(2).Infof("Error applying project sleep quota: %s\n", err)
		}
		return
	}

	// Iterate through RCs to calculate runtime
	var dcOverQuota []string
	// Maps an RC to if it has been scaled, eg by its DC being scaled
	rcOverQuota := make(map[string]bool)
	dcs := make(map[string]time.Duration)
	namespaceAndKind = namespace + "/" + RCKind
	rcs, err := s.resources.ByIndex("byNamespaceAndKind", namespaceAndKind)
	if err != nil {
		glog.Errorf("Error getting project (%s) RC resources:", namespace, err)
		return
	}
	for _, obj := range rcs {
		rc := obj.(*ResourceObject)
		totalRuntime := rc.GetResourceRuntime(s.config.Period)
		if s.PruneResource(rc) {
			continue
		}
		if totalRuntime >= s.config.Quota {
			glog.V(2).Infof("RC %s over quota in project %s\n", rc.Name, namespace)
			rcOverQuota[rc.Name] = true
		}
		if _, ok := dcs[rc.DeploymentConfig]; ok {
			dcs[rc.DeploymentConfig] += totalRuntime
		} else {
			dcs[rc.DeploymentConfig] = totalRuntime
		}
	}
	// Iterate through DCs to calculate cumulative runtime
	for dc, runningTime := range dcs {
		if runningTime >= s.config.Quota {
			glog.V(2).Infof("DC %s over quota in project %s\n", dc, namespace)
			dcOverQuota = append(dcOverQuota, dc)
		}
	}
	for _, name := range dcOverQuota {
		// Scale down dcs that are over quota
		glog.V(2).Infof("Scaling DC %s in project (%s)\n", name, namespace)
		_, err := s.osClient.DeploymentConfigs(namespace).Get(name)
		if err != nil {
			glog.V(0).Infof("Error getting DC %s/%s: %s\n", name, namespace, err)
		}

		// Scale RCs related to DC
		dcRCs, err := s.resources.ByIndex("rcByDC", name)
		if err != nil {
			glog.Errorf("Error getting RCs for DC %s: %s", name, err)
		}
		for _, obj := range dcRCs {
			rc := obj.(*ResourceObject)
			rcOverQuota[rc.Name] = true
		}
	}
	for name := range rcOverQuota {
		// Scale down dcs that are over quota
		if rcOverQuota[name] {
			err = s.scaleRC(name, namespace)
			if err != nil {
				glog.Errorf("Error scaling RC %s: %s", name, err)
				continue
			}
		}
	}

	// Project sort index:
	s.updateProjectSortIndex(namespace, quotaSecondsConsumed)
}

func (s *Sleeper) scaleRC(name, namespace string) error {
	glog.V(2).Infof("Scaling RC %s in project (%s)\n", name, namespace)
	thisRC, err := s.kubeClient.ReplicationControllers(namespace).Get(name)
	if err != nil {
		return err
	}
	thisRC.Spec.Replicas = 0
	_, err = s.kubeClient.ReplicationControllers(namespace).Update(thisRC)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sleeper) updateProjectSortIndex(namespace string, quotaSeconds float64) {
	obj, err := s.resources.ByIndex("getProject", namespace)
	if err != nil {
		glog.Errorf("Error getting project resources: %s", err)
	}

	if len(obj) == 1 {
		project := obj[0].(*ResourceObject)
		// Projects closer to force-sleep will have a lower index value
		sortIndex := -1 * quotaSeconds
		project.ProjectSortIndex = sortIndex
		s.resources.Update(project)
	}
}

// Check to clear cached resources whose runtimes are outside of the period, and thus irrelevant
func (s *Sleeper) PruneResource(resource *ResourceObject) bool {
	count := len(resource.RunningTimes)
	if count < 1 {
		return true
	}
	if resource.isStarted() {
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

func (r *ResourceObject) GetResourceRuntime(period time.Duration) time.Duration {
	var total time.Duration
	count := len(r.RunningTimes) - 1
	outsidePeriod := 0

	for i := count; i >= 0; i-- {
		if i == count && r.isStarted() {
			// special case to see if object is currently running
			// if running && startTime > period, then it's been running for period
			if time.Since(r.RunningTimes[i].Start) > period {
				total += period
			} else {
				total += time.Now().Sub(r.RunningTimes[i].Start)
			}
			continue
		}
		if time.Since(r.RunningTimes[i].End) > period {
			// End time is outside period
			outsidePeriod = i
			break
		} else if time.Since(r.RunningTimes[i].Start) > period {
			// Start time is outside period
			total += r.RunningTimes[i].End.Sub(time.Now().Add(-1 * period))
		} else {
			total += r.RunningTimes[i].End.Sub(r.RunningTimes[i].Start)
		}
	}

	// Remove running times outside of period
	r.RunningTimes = r.RunningTimes[outsidePeriod:]
	return total
}

func getQuotaSeconds(seconds float64, request, limit resource.Quantity) float64 {
	requestVal := float64(request.Value())
	limitVal := float64(limit.Value())
	var percentage float64
	percentage = requestVal / limitVal
	return seconds * percentage
}

func resourceKey(obj interface{}) (string, error) {
	return string(obj.(*ResourceObject).UID), nil
}

func indexResourceByNamespace(obj interface{}) ([]string, error) {
	return []string{obj.(*ResourceObject).Namespace}, nil
}

func getProjectResource(obj interface{}) ([]string, error) {
	if obj.(*ResourceObject).Kind == ProjectKind {
		return []string{obj.(*ResourceObject).Name}, nil
	}
	return []string{}, nil
}

func getRCByDC(obj interface{}) ([]string, error) {
	if obj.(*ResourceObject).Kind == RCKind {
		return []string{obj.(*ResourceObject).DeploymentConfig}, nil
	}
	return []string{}, nil
}

func getAllResourcesOfKind(obj interface{}) ([]string, error) {
	return []string{obj.(*ResourceObject).Kind}, nil
}

func indexResourceByNamespaceAndKind(obj interface{}) ([]string, error) {
	object := obj.(*ResourceObject)
	fullName := object.Namespace + "/" + object.Kind
	return []string{fullName}, nil
}

func indexResourceByFullName(obj interface{}) ([]string, error) {
	object := obj.(*ResourceObject)
	fullName := object.Namespace + "/" + object.Kind + "/" + object.Name
	return []string{fullName}, nil
}
