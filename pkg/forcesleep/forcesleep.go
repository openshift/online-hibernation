package forcesleep

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	osclient "github.com/openshift/origin/pkg/client"
	"github.com/openshift/origin/pkg/client/cache"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"
	deployapi "github.com/openshift/origin/pkg/deploy/api"
	unidlingapi "github.com/openshift/origin/pkg/unidling/api"

	"github.com/golang/glog"
	"github.com/spf13/pflag"

	kapi "k8s.io/kubernetes/pkg/api"
	kerrors "k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/api/unversioned"
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
	kubeClient        kclient.Interface
	osClient          osclient.Interface
	factory           *clientcmd.Factory
	config            *SleeperConfig
	resources         kcache.Indexer
	projectSleepQuota runtime.Object
	stopChannel       <-chan struct{}
}

type ResourceObject struct {
	Mutex             sync.RWMutex
	UID               types.UID
	Name              string
	Namespace         string
	Kind              string
	Terminating       bool
	ResourceVersion   string
	RunningTimes      []*RunningTime
	MemoryRequested   resource.Quantity
	DeploymentConfig  string
	LastSleepTime     time.Time
	ProjectSortIndex  float64
	DeletionTimestamp time.Time
	Selectors         map[string]string
	Labels            map[string]string
	Annotations       map[string]string
}

type RunningTime struct {
	Start time.Time
	End   time.Time
}

type watchListItem struct {
	objType   runtime.Object
	watchFunc func(options kapi.ListOptions) (watch.Interface, error)
}

type ControllerScaleReference struct {
	Kind     string
	Name     string
	Replicas int32
}

func newResourceFromPod(pod *kapi.Pod) *ResourceObject {
	terminating := false
	if (pod.Spec.RestartPolicy != kapi.RestartPolicyAlways) && (pod.Spec.ActiveDeadlineSeconds != nil) {
		terminating = true
	}

	resource := &ResourceObject{
		UID:             pod.ObjectMeta.UID,
		Name:            pod.ObjectMeta.Name,
		Namespace:       pod.ObjectMeta.Namespace,
		Kind:            PodKind,
		Terminating:     terminating,
		ResourceVersion: pod.ObjectMeta.ResourceVersion,
		MemoryRequested: pod.Spec.Containers[0].Resources.Requests["memory"],
		RunningTimes:    make([]*RunningTime, 0),
		Labels:          make(map[string]string),
		Annotations:     make(map[string]string),
	}

	for k, v := range pod.ObjectMeta.Labels {
		resource.Labels[k] = v
	}

	for k, v := range pod.ObjectMeta.Annotations {
		resource.Annotations[k] = v
	}

	if pod.ObjectMeta.DeletionTimestamp.IsZero() {
		resource.DeletionTimestamp = time.Time{}
	} else {
		resource.DeletionTimestamp = pod.ObjectMeta.DeletionTimestamp.Time
	}
	return resource
}

func newResourceFromRC(rc *kapi.ReplicationController) *ResourceObject {
	resource := &ResourceObject{
		UID:              rc.ObjectMeta.UID,
		Name:             rc.ObjectMeta.Name,
		Namespace:        rc.ObjectMeta.Namespace,
		Kind:             RCKind,
		ResourceVersion:  rc.ObjectMeta.ResourceVersion,
		DeploymentConfig: rc.ObjectMeta.Annotations[OpenShiftDCName],
		RunningTimes:     make([]*RunningTime, 0),
		Selectors:        make(map[string]string),
		Labels:           make(map[string]string),
	}

	for k, v := range rc.Spec.Selector {
		resource.Selectors[k] = v
	}

	for k, v := range rc.ObjectMeta.Labels {
		resource.Labels[k] = v
	}

	if rc.ObjectMeta.DeletionTimestamp.IsZero() {
		resource.DeletionTimestamp = time.Time{}
	} else {
		resource.DeletionTimestamp = rc.ObjectMeta.DeletionTimestamp.Time
	}
	return resource
}

func newResourceFromService(svc *kapi.Service) *ResourceObject {
	resource := &ResourceObject{
		UID:             svc.ObjectMeta.UID,
		Name:            svc.ObjectMeta.Name,
		Namespace:       svc.ObjectMeta.Namespace,
		Kind:            ServiceKind,
		ResourceVersion: svc.ObjectMeta.ResourceVersion,
		Selectors:       make(map[string]string),
	}

	for k, v := range svc.Spec.Selector {
		resource.Selectors[k] = v
	}

	return resource
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

func newResourceFromInterface(resource interface{}) *ResourceObject {
	switch r := resource.(type) {
	case *kapi.Pod:
		return newResourceFromPod(r)
	case *kapi.ReplicationController:
		return newResourceFromRC(r)
	case *kapi.Service:
		return newResourceFromService(r)
	}
	return nil
}

// Checks if we have a ResourceObject in cache for requested project
// If not, a new ResourceObject is created for that project
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

func NewSleeper(osClient osclient.Interface, kubeClient kclient.Interface, sc *SleeperConfig, f *clientcmd.Factory) *Sleeper {
	ctrl := &Sleeper{
		osClient:   osClient,
		kubeClient: kubeClient,
		config:     sc,
		factory:    f,
		resources: kcache.NewIndexer(resourceKey, kcache.Indexers{
			"byNamespace":        indexResourceByNamespace,
			"byNamespaceAndKind": indexResourceByNamespaceAndKind,
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

	svcLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return s.kubeClient.Services(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return s.kubeClient.Services(kapi.NamespaceAll).Watch(options)
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

// Checks if resource exists in project cache
func (s *Sleeper) resourceInCache(UID string) bool {
	_, exists, err := s.resources.GetByKey(UID)
	if err != nil {
		return false
	}
	return exists
}

// Adds a new runningtime start value to a resource object
func (s *Sleeper) startResource(r *ResourceObject) {
	s.createProjectInCache(r.Namespace)
	UID := string(r.UID)
	if !s.resourceInCache(UID) {
		s.resources.Add(r)
	}

	// Make sure object was successfully created before working on it
	obj, exists, err := s.resources.GetByKey(UID)
	if err != nil {
		glog.Errorf("Error starting resource by UID: %s", err)
		return
	}
	if exists {
		resource := obj.(*ResourceObject)
		glog.V(3).Infof("Starting resource: %s\n", resource.Name)
		if !resource.isStarted() {
			runTime := &RunningTime{
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
func (s *Sleeper) stopResource(r *ResourceObject) {
	var stopTime time.Time

	s.createProjectInCache(r.Namespace)
	resourceTime := r.DeletionTimestamp
	UID := string(r.UID)

	// See if we already have a reference to this resource in cache
	if s.resourceInCache(UID) {
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

	obj, exists, err := s.resources.GetByKey(UID)
	if err != nil {
		glog.Errorf("Error stopping resource by UID: %s", err)
		return
	}
	if exists {
		resource := obj.(*ResourceObject)
		glog.V(3).Infof("Stopping resource: %s\n", resource.Name)
		runTimeCount := len(resource.RunningTimes)

		if resource.isStarted() {
			resource.RunningTimes[runTimeCount-1].End = stopTime
			s.resources.Update(resource)
		}
	} else {
		glog.Errorf("Error stopping resource: could not find resource %s\n", UID)
	}
}

// Checks to see if a resource is stopped in our cache
// Ie, the resource's most recent RunningTime has a specified End time
func (r *ResourceObject) isStopped() bool {
	runtimes := len(r.RunningTimes)
	if runtimes == 0 {
		return true
	} else {
		return !(r.RunningTimes[runtimes-1].End.IsZero())
	}

}

func (r *ResourceObject) isStarted() bool {
	return !r.isStopped()
}

// Handles incoming events for resources of any type
func (s *Sleeper) handleResource(eventType watch.EventType, resource interface{}) error {
	switch eventType {
	case watch.Added:
	case watch.Modified:
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
		r := newResourceFromInterface(resource)
		if r != nil {
			s.stopResource(r)
		}
	}
	return nil
}

// Function for handling Pod ADD/MODIFY events
func (s *Sleeper) handlePodChange(pod *kapi.Pod) {
	glog.V(3).Infof("Received ADD/MODIFY for pod: %s\n", pod.Name)
	resourceObject := newResourceFromPod(pod)

	// If the pod is running, make sure it's started in cache
	// Otherwise, make sure it's stopped
	switch pod.Status.Phase {
	case kapi.PodRunning:
		s.startResource(resourceObject)
	case kapi.PodSucceeded:
	case kapi.PodFailed:
	case kapi.PodUnknown:
		s.stopResource(resourceObject)
	}
}

// Function for handling RC ADD/MODIFY events
func (s *Sleeper) handleRCChange(rc *kapi.ReplicationController) {
	glog.V(3).Infof("Received ADD/MODIFY for RC: %s\n", rc.Name)
	resourceObject := newResourceFromRC(rc)

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
	r := newResourceFromService(svc)
	s.createProjectInCache(r.Namespace)
	if !s.resourceInCache(string(r.UID)) {
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
			glog.Errorf("Error setting LastSleepTime for project %s: %s\n", namespace, err)
		}

		quotaInterface := s.kubeClient.ResourceQuotas(namespace)
		quota := s.projectSleepQuota.(*kapi.ResourceQuota)
		_, err := quotaInterface.Create(quota)
		if err != nil {
			return err
		}

	}

	failed := false

	glog.V(2).Infof("Idling services in project %s\n", namespace)
	err = s.idleProjectServices(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Error idling services in project %s: %s\n", namespace, err)
	}

	glog.V(2).Infof("Scaling RCs in project %s\n", namespace)
	err = s.scaleProjectRCs(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Error scaling RCs in project %s: %s\n", namespace, err)
	}

	glog.V(2).Infof("Scaling DCs in project %s\n", namespace)
	err = s.scaleProjectDCs(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Error scaling DCs in project %s: %s\n", namespace, err)
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
		r := resource.(*ResourceObject)
		if r.Kind != ProjectKind && r.Kind != ServiceKind {
			s.resources.Delete(r)
		}
	}
	return nil
}

func (s *Sleeper) getAndCopyEndpoint(namespace, name string) (*kapi.Endpoints, error) {
	endpointInterface := s.kubeClient.Endpoints(namespace)
	endpoint, err := endpointInterface.Get(name)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error annotating Endpoint in namespace %s: %s\n", namespace, err))
	}
	copy, err := kapi.Scheme.DeepCopy(endpoint)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error annotating Endpoint in namespace %s: %s\n", namespace, err))
	}
	newEndpoint := copy.(*kapi.Endpoints)

	if newEndpoint.Annotations == nil {
		newEndpoint.Annotations = make(map[string]string)
	}
	return newEndpoint, nil
}

// The goal with this is to re-create `oc idle` to the extent we need to, in order to provide automatic re-scaling on project wake
// `oc idle` links controllers to endpoints by:
//   1. taking in an endpoint (service name)
//   2. getting the pods on that endpoint
//   3. getting the controller on each pod (and, in the case of DCs, getting the controller on that controller)
// This approach is:
//   1. loop through all services in project
//   2. for each service, get pods on service by label selector
//   3. get the controller on each pod (and, in the case of DCs, get the controller on that controller)
//   4. get endpoint with the same name as the service
func (s *Sleeper) idleProjectServices(namespace string) error {
	failed := false
	endpointInterface := s.kubeClient.Endpoints(namespace)
	svcs, err := s.getProjectServices(namespace)
	if err != nil {
		return err
	}

	projectPods, err := s.getProjectPods(namespace)
	if err != nil {
		return err
	}

	// Loop through all of the services in a namespace
	for _, obj := range svcs {
		svc := obj.(*ResourceObject)

		// First we must find the parent controller for each pod
		pods := s.getPodsForService(svc, projectPods)
		resourceRefs, err := s.findScalableResourcesForService(pods)
		if err != nil {
			glog.Errorf("Error scaling service: %s\n", err)
			failed = true
			continue
		}

		// Now we add a previous-scale annotation to each parent controller, and also get the reference
		// annotation that will be added to the endpoint later
		scaleRefs := make(map[kapi.ObjectReference]*ControllerScaleReference)
		for ref := range resourceRefs {
			scaleRef, err := s.annotateController(ref, time.Time{})
			if err != nil {
				glog.Errorf("Error annotating controller: %s\n", err)
				failed = true
				continue
			}
			scaleRefs[ref] = scaleRef
		}

		// Now annotate the endpoints
		var endpointScaleRefs []*ControllerScaleReference
		for _, scaleRef := range scaleRefs {
			endpointScaleRefs = append(endpointScaleRefs, scaleRef)
		}
		scaleRefsBytes, err := json.Marshal(endpointScaleRefs)
		if err != nil {
			failed = true
			glog.Errorf("Error annotating Endpoint in namespace %s: %s\n", namespace, err)
			continue
		}
		newEndpoint, err := s.getAndCopyEndpoint(namespace, svc.Name)
		if err != nil {
			failed = true
			glog.Errorf("Error annotating Endpoint in namespace %s: %s\n", namespace, err)
			continue
		}
		newEndpoint.Annotations[unidlingapi.UnidleTargetAnnotation] = string(scaleRefsBytes)

		// Need to delete any previous IdledAtAnnotations to prevent premature unidling
		if newEndpoint.Annotations[unidlingapi.IdledAtAnnotation] != "" {
			delete(newEndpoint.Annotations, unidlingapi.IdledAtAnnotation)
		}

		_, err = endpointInterface.Update(newEndpoint)
		if err != nil {
			glog.Errorf("Error marshalling endpoint scale reference while annotating Endpoint in namespace %s: %s\n", namespace, err)
			failed = true
		}
	}

	if failed {
		return errors.New("Failed to idle all project services")
	}
	return nil
}

func (s *Sleeper) getProjectServices(namespace string) ([]interface{}, error) {
	namespaceAndKind := namespace + "/" + ServiceKind
	svcs, err := s.resources.ByIndex("byNamespaceAndKind", namespaceAndKind)
	if err != nil {
		return nil, err
	}
	return svcs, nil
}

func (s *Sleeper) getProjectPods(namespace string) ([]interface{}, error) {
	namespaceAndKind := namespace + "/" + PodKind
	pods, err := s.resources.ByIndex("byNamespaceAndKind", namespaceAndKind)
	if err != nil {
		return nil, err
	}
	return pods, nil
}

// Takes in a list of locally cached Pod ResourceObjects and a Service ResourceObject
// Compares selector labels on the pods and services to check for any matches which link the two
func (s *Sleeper) getPodsForService(svc *ResourceObject, podList []interface{}) map[string]runtime.Object {
	pods := make(map[string]runtime.Object)
	for _, obj := range podList {
		pod := obj.(*ResourceObject)
		for podKey, podVal := range pod.Labels {
			for svcKey, svcVal := range svc.Selectors {
				if _, exists := pods[pod.Name]; !exists && svcKey == podKey && svcVal == podVal {
					podRef, err := s.kubeClient.Pods(pod.Namespace).Get(pod.Name)
					if err != nil {
						// This may fail when a pod has been deleted but not yet pruned from cache
						glog.Errorf("Error getting pod in namespace %s: %s\n", pod.Namespace, err)
						continue
					}
					pods[pod.Name] = podRef
				}
			}
		}
	}
	return pods
}

// Returns an ObjectReference to the parent controller (RC/DC) for a resource
func getControllerRef(obj runtime.Object) (*kapi.ObjectReference, error) {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}

	annotations := objMeta.GetAnnotations()

	creatorRefRaw, creatorListed := annotations[kapi.CreatedByAnnotation]
	if !creatorListed {
		// if we don't have a creator listed, try the openshift-specific Deployment annotation
		dcName, dcNameListed := annotations[deployapi.DeploymentConfigAnnotation]
		if !dcNameListed {
			return nil, nil
		}

		return &kapi.ObjectReference{
			Name:      dcName,
			Namespace: objMeta.GetNamespace(),
			Kind:      "DeploymentConfig",
		}, nil
	}

	serializedRef := &kapi.SerializedReference{}
	err = json.Unmarshal([]byte(creatorRefRaw), serializedRef)
	if err != nil {
		return nil, err
	}
	return &serializedRef.Reference, nil
}

// Returns a generic runtime.Object for a controller
func getController(ref kapi.ObjectReference, f *clientcmd.Factory) (runtime.Object, error) {
	mapper, _ := f.Object(true)
	gv, err := unversioned.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil, err
	}
	var mapping *meta.RESTMapping
	mapping, err = mapper.RESTMapping(unversioned.GroupKind{Group: gv.Group, Kind: ref.Kind}, "")
	if err != nil {
		return nil, err
	}
	var client ctlresource.RESTClient
	client, err = f.ClientForMapping(mapping)
	if err != nil {
		return nil, err
	}
	helper := ctlresource.NewHelper(client, mapping)

	var controller runtime.Object
	controller, err = helper.Get(ref.Namespace, ref.Name, false)
	if err != nil {
		return nil, err
	}

	return controller, nil
}

// Takes a list of Pods and looks at their parent controllers
// Then takes that list of parent controllers and checks if there is another parent above them
// ex. pod -> RC -> DC, DC is the main parent controller we want to idle
func (s *Sleeper) findScalableResourcesForService(pods map[string]runtime.Object) (map[kapi.ObjectReference]struct{}, error) {
	immediateControllerRefs := make(map[kapi.ObjectReference]struct{})
	for _, pod := range pods {
		controllerRef, err := getControllerRef(pod)
		if err != nil {
			return nil, err
		}
		immediateControllerRefs[*controllerRef] = struct{}{}
	}

	controllerRefs := make(map[kapi.ObjectReference]struct{})
	for controllerRef := range immediateControllerRefs {
		controller, err := getController(controllerRef, s.factory)
		if err != nil {
			return nil, err
		}

		if controller != nil {
			var parentControllerRef *kapi.ObjectReference
			parentControllerRef, err = getControllerRef(controller)
			if err != nil {
				return nil, fmt.Errorf("unable to load the creator of %s %q: %v", controllerRef.Kind, controllerRef.Name, err)
			}

			if parentControllerRef == nil {
				controllerRefs[controllerRef] = struct{}{}
			} else {
				controllerRefs[*parentControllerRef] = struct{}{}
			}
		}
	}
	return controllerRefs, nil
}

// Adds PreviousScaleAnnotations (for applying force-sleep) and IdledAtAnnotations (removing force-sleep)
// Based on whether an IdledAtTime is being passed in
func (s *Sleeper) annotateController(ref kapi.ObjectReference, nowTime time.Time) (*ControllerScaleReference, error) {
	obj, err := getController(ref, s.factory)
	if err != nil {
		return nil, err
	}

	var replicas int32
	switch controller := obj.(type) {
	case *deployapi.DeploymentConfig:
		dcInterface := s.osClient.DeploymentConfigs(controller.Namespace)
		copy, err := kapi.Scheme.DeepCopy(controller)
		if err != nil {
			return nil, err
		}
		newDC := copy.(*deployapi.DeploymentConfig)
		if !nowTime.IsZero() {
			newDC.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		} else {
			replicas = controller.Spec.Replicas
			newDC.Annotations[unidlingapi.PreviousScaleAnnotation] = fmt.Sprintf("%v", controller.Spec.Replicas)
			// Need to remove reference to a previous idling, if it exists
			if newDC.Annotations[unidlingapi.IdledAtAnnotation] != "" {
				delete(newDC.Annotations, unidlingapi.IdledAtAnnotation)
			}
		}

		_, err = dcInterface.Update(newDC)
		if err != nil {
			if kerrors.IsNotFound(err) {
				return nil, nil
			} else {
				return nil, errors.New(fmt.Sprintf("Error annotating DC in namespace %s: %s\n", controller.Namespace, err))
			}
		}

	case *kapi.ReplicationController:
		rcInterface := s.kubeClient.ReplicationControllers(controller.Namespace)

		copy, err := kapi.Scheme.DeepCopy(controller)
		if err != nil {
			return nil, err
		}
		newRC := copy.(*kapi.ReplicationController)
		if !nowTime.IsZero() {
			newRC.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		} else {
			replicas = controller.Spec.Replicas
			newRC.Annotations[unidlingapi.PreviousScaleAnnotation] = fmt.Sprintf("%v", controller.Spec.Replicas)
			// Need to remove reference to a previous idling, if it exists
			if newRC.Annotations[unidlingapi.IdledAtAnnotation] != "" {
				delete(newRC.Annotations, unidlingapi.IdledAtAnnotation)
			}
		}

		_, err = rcInterface.Update(newRC)
		if err != nil {
			if kerrors.IsNotFound(err) {
				return nil, nil
			} else {
				return nil, errors.New(fmt.Sprintf("Error annotating RC in namespace %s: %s\n", controller.Namespace, err))
			}
		}
	}
	return &ControllerScaleReference{
		Kind:     ref.Kind,
		Name:     ref.Name,
		Replicas: replicas}, nil
}

func (s *Sleeper) scaleProjectDCs(namespace string) error {
	dcInterface := s.osClient.DeploymentConfigs(namespace)
	dcList, err := dcInterface.List(kapi.ListOptions{})
	if err != nil {
		return err
	}

	failed := false
	for _, dc := range dcList.Items {
		// Scale down DC
		dc.Spec.Replicas = 0
		_, err = dcInterface.Update(&dc)
		if err != nil {
			if kerrors.IsNotFound(err) {
				continue
			} else {
				glog.Errorf("Error scaling DC in namespace %s: %s\n", namespace, err)
				failed = true
			}
		}

	}
	if failed {
		return errors.New("Failed to scale all project DCs")
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
	failed := false
	for _, thisRC := range rcList.Items {
		thisRC.Spec.Replicas = 0
		_, err = rcInterface.Update(&thisRC)
		if err != nil {
			if kerrors.IsNotFound(err) {
				continue
			} else {
				glog.Errorf("Error scaling RC in namespace %s: %s\n", namespace, err)
				failed = true
			}
		}
	}
	if failed {
		return errors.New("Failed to scale all project RCs")
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

func (s *Sleeper) wakeProject(project *ResourceObject) bool {
	namespace := project.Namespace
	if !project.LastSleepTime.IsZero() {
		if time.Since(project.LastSleepTime) > s.config.ProjectSleepPeriod {
			// First remove the force-sleep pod count resource quota from the project
			glog.V(2).Infof("Removing sleep quota for project %s\n", namespace)
			quotaInterface := s.kubeClient.ResourceQuotas(namespace)
			err := quotaInterface.Delete(ProjectSleepQuotaName)
			if err != nil {
				if kerrors.IsNotFound(err) {
					glog.V(0).Infof("Error removing sleep quota: %s", err)
				} else {
					glog.Errorf("Error removing sleep quota: %s", err)
				}
			}
			project.LastSleepTime = time.Time{}
			s.resources.Update(project)

			err = s.unidleProjectServices(namespace)
			if err != nil {
				glog.Errorf("Error applying service unidling: %s", err)
			}
			return true
		}
	}
	return false
}

// This is the "second part" of replicating `oc idle` (first part applied in IdleProjectServices() when put to sleep)
// which we apply when waking projects:
// Adds an "idled-at" annotation to Endpoints and Scalable Resources so that traffic will trigger the
// idling controller to wake them
func (s *Sleeper) unidleProjectServices(namespace string) error {
	glog.V(2).Infof("Adding idled-at annotations for project services %s\n", namespace)
	endpointInterface := s.kubeClient.Endpoints(namespace)
	svcs, err := s.getProjectServices(namespace)
	if err != nil {
		return errors.New(fmt.Sprintf("Error getting project services: %s", err))
	}

	nowTime := time.Now().UTC()

	failed := false
	// Loop through all of the services in a namespace
	for _, obj := range svcs {
		svc := obj.(*ResourceObject)

		// Get the endpoint object for that service
		newEndpoint, err := s.getAndCopyEndpoint(namespace, svc.Name)
		if err != nil {
			glog.Errorf("Error getting Endpoint in namespace %s: %s\n", namespace, err)
			failed = true
			continue
		}
		newEndpoint.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)

		_, err = endpointInterface.Update(newEndpoint)
		if err != nil {
			glog.Errorf("Error annotating Endpoint in namespace %s: %s\n", namespace, err)
			failed = true
			continue
		}

		// Now get controllers for that service
		// And add an idled-at annotation to those resources
		scaleRefsBytes := newEndpoint.Annotations[unidlingapi.UnidleTargetAnnotation]
		var scaleRefs []ControllerScaleReference
		err = json.Unmarshal([]byte(scaleRefsBytes), &scaleRefs)
		if err != nil {
			glog.Errorf("Error annotating DC in namespace %s when unmarshalling scaleRefsBytes from endpoint: %s\n", namespace, err)
			failed = true
			continue
		}

		for _, scaleRef := range scaleRefs {
			ref := kapi.ObjectReference{
				Name:      scaleRef.Name,
				Kind:      scaleRef.Kind,
				Namespace: svc.Namespace,
			}
			_, err := s.annotateController(ref, nowTime)
			if err != nil {
				glog.Errorf("Error annotating controller: %s\n", err)
				failed = true
				continue
			}
		}
	}
	if failed {
		return errors.New("Failed to add unidling annotations to all project services")
	}
	return nil
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
	pods, err := s.getProjectPods(namespace)
	if err != nil {
		glog.Errorf("Error getting project (%s) pod resources:", namespace, err)
	}
	termQuotaSecondsConsumed := 0.0
	nonTermQuotaSecondsConsumed := 0.0
	for _, obj := range pods {
		pod := obj.(*ResourceObject)
		if s.PruneResource(pod) {
			continue
		}
		totalRuntime, changed := pod.GetResourceRuntime(s.config.Period)
		if changed {
			s.resources.Update(pod)
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

	// Iterate through RCs to calculate runtime
	var dcOverQuota []string
	// Maps an RC to if it has been scaled, eg by its DC being scaled
	rcOverQuota := make(map[string]bool)
	dcs := make(map[string]time.Duration)
	namespaceAndKind := namespace + "/" + RCKind
	rcs, err := s.resources.ByIndex("byNamespaceAndKind", namespaceAndKind)
	if err != nil {
		glog.Errorf("Error getting project (%s) RC resources:", namespace, err)
		return
	}
	for _, obj := range rcs {
		rc := obj.(*ResourceObject)
		if s.PruneResource(rc) {
			continue
		}
		totalRuntime, changed := rc.GetResourceRuntime(s.config.Period)
		if changed {
			s.resources.Update(rc)
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
		glog.V(3).Infof("Scaling DC %s in project (%s)\n", name, namespace)

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
		err = s.scaleDownRC(name, namespace)
		if err != nil {
			glog.Errorf("Error scaling RC %s: %s", name, err)
			continue
		}
	}

	// Project sort index:
	s.updateProjectSortIndex(namespace, quotaSecondsConsumed)
}

func (s *Sleeper) scaleDownRC(name, namespace string) error {
	glog.V(3).Infof("Scaling RC %s in project (%s)\n", name, namespace)
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

func (r *ResourceObject) GetResourceRuntime(period time.Duration) (time.Duration, bool) {
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
	changed := false
	r.RunningTimes = r.RunningTimes[outsidePeriod:]
	// Let sync function know if need to update cache with new ResourceObject for removed times
	if len(r.RunningTimes) < count {
		changed = true
	}
	return total, changed
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
