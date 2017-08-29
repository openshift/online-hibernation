package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"

	osclient "github.com/openshift/origin/pkg/client"

	kapi "k8s.io/kubernetes/pkg/api"
	kerrors "k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/api/resource"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/types"
)

const LastSleepTimeAnnotation = "openshift.io/last-sleep-time"

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
	IsAsleep          bool
}

type resourceStore struct {
	mu sync.RWMutex
	kcache.Indexer
	Exclude    map[string]bool
	OsClient   osclient.Interface
	KubeClient kclient.Interface
}

func (s *resourceStore) NewResourceFromInterface(resource interface{}) *ResourceObject {
	switch r := resource.(type) {
	case *kapi.Pod:
		return s.NewResourceFromPod(r)
	case *kapi.ReplicationController:
		return s.NewResourceFromRC(r)
	case *kapi.Service:
		return s.NewResourceFromService(r)
	case *kapi.Namespace:
		return s.NewResourceFromProject(r)
	}
	return nil
}

func (s *resourceStore) AddOrModify(obj interface{}) error {
	switch r := obj.(type) {
	case *kapi.Pod:
		resObj := s.NewResourceFromInterface(obj.(*kapi.Pod))
		glog.V(3).Infof("Received ADD/MODIFY for pod: %s\n", resObj.Name)
		// If the pod is running, make sure it's started in cache
		// Otherwise, make sure it's stopped
		switch r.Status.Phase {
		case kapi.PodRunning:
			s.startResource(resObj)
		case kapi.PodSucceeded, kapi.PodFailed, kapi.PodUnknown:
			s.stopResource(resObj)
		}
	case *kapi.ReplicationController:
		resObj := s.NewResourceFromInterface(obj.(*kapi.ReplicationController))
		glog.V(3).Infof("Received ADD/MODIFY for RC: %s\n", resObj.Name)
		// If RC has more than 0 active replicas, make sure it's started in cache
		if r.Status.Replicas > 0 {
			s.startResource(resObj)
		} else { // replicas == 0
			s.stopResource(resObj)
		}
	case *kapi.Service:
		resObj := s.NewResourceFromInterface(obj.(*kapi.Service))
		glog.V(3).Infof("Received ADD/MODIFY for Service: %s\n", resObj.Name)
		UID := string(resObj.UID)
		obj, exists, err := s.GetResourceByKey(UID)
		if err != nil {
			return err
		}
		if exists {
			res, err := kapi.Scheme.DeepCopy(obj)
			if err != nil {
				return err
			}
			resource := res.(*ResourceObject)
			resource.Kind = ServiceKind
			return s.Indexer.Update(resource)
		} else {
			s.Indexer.Add(resObj)
		}
	case *kapi.Namespace:
		resObj := s.NewResourceFromInterface(obj.(*kapi.Namespace))
		glog.V(3).Infof("Received ADD/MODIFY for Project: %s\n", r.Name)
		if r.Status.Phase == kapi.NamespaceActive {
			_, exists, err := s.GetResourceByKey(r.Name)
			if err != nil {
				glog.Errorf("Error: %v", err)
			}
			if !exists {
				if !s.Exclude[r.Name] {
					resObj := s.NewResourceFromProject(r)
					s.Indexer.Add(resObj)
				}
			}
		}
		// Now make sure it was added
		UID := string(resObj.UID)
		obj, exists, err := s.GetResourceByKey(UID)
		if err != nil {
			return err
		}
		if exists {
			res, err := kapi.Scheme.DeepCopy(obj)
			if err != nil {
				return err
			}
			resource := res.(*ResourceObject)
			return s.Indexer.Update(resource)
		} else {
			if r.Status.Phase == kapi.NamespaceActive && !s.Exclude[r.Name] {
				glog.Errorf("Project not in cache")
			}
		}
	default:
		glog.Errorf("Object not recognized")
	}
	return nil
}

func (s *resourceStore) DeleteKapiResource(obj interface{}) error {
	glog.V(3).Infof("Received DELETE event\n")
	switch r := obj.(type) {
	case *kapi.Pod, *kapi.Service, *kapi.ReplicationController:
		resObj := s.NewResourceFromInterface(r)
		UID := string(resObj.UID)
		if s.ResourceInCache(UID) {
			s.stopResource(resObj)
		}
	case *kapi.Namespace:
		resObj := s.NewResourceFromInterface(r)
		// Get all resources from a deleted namespace and remove them from the cache
		resources, err := s.Indexer.ByIndex("byNamespace", resObj.Name)
		if err != nil {
			return err
		}
		for _, res := range resources {
			r := res.(*ResourceObject)
			if err := s.Indexer.Delete(r); err != nil {
				return err
			}
		}
	default:
		glog.V(3).Infof("Object not recognized, Could not delete object")
	}
	return nil
}

func (s *resourceStore) Add(obj interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r := obj.(type) {
	case *kapi.Namespace, *kapi.Service, *kapi.Pod, *kapi.ReplicationController:
		if err := s.AddOrModify(r); err != nil {
			return fmt.Errorf("Error: %s", err)
		}
	default:
		glog.Errorf("Object not recognized")
	}
	return nil
}

func (s *resourceStore) Delete(obj interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch obj.(type) {
	case *kapi.Namespace, *kapi.Service, *kapi.Pod, *kapi.ReplicationController:
		if err := s.DeleteKapiResource(obj); err != nil {
			return fmt.Errorf("Error: %s", err)
		}
	default:
		glog.Errorf("Object not recognized")
	}
	return nil
}

func (s *resourceStore) Update(obj interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r := obj.(type) {
	case *kapi.Namespace, *kapi.Service, *kapi.Pod, *kapi.ReplicationController:
		if err := s.AddOrModify(r); err != nil {
			return fmt.Errorf("Error: %s", err)
		}
	default:
		glog.Errorf("Object not recognized")
	}
	return nil
}

func (s *resourceStore) UpdateResourceObject(obj *ResourceObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Indexer.Update(obj)
}

func (s *resourceStore) AddResourceObject(obj *ResourceObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Indexer.Add(obj)
}

func (s *resourceStore) DeleteResourceObject(obj *ResourceObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch obj.Kind {
	case PodKind, RCKind, ServiceKind:
		UID := string(obj.UID)
		if s.ResourceInCache(UID) {
			// Should this be stopResource?
			s.Indexer.Delete(obj)
		}
	case ProjectKind:
		resources, err := s.Indexer.ByIndex("byNamespace", obj.Name)
		if err != nil {
			return err
		}
		for _, res := range resources {
			r := res.(*ResourceObject)
			if err := s.Indexer.Delete(r); err != nil {
				return err
			}
		}
	default:
		glog.Errorf("Object not recognized")
	}
	return nil
}

func (s *resourceStore) List() []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.List()
}

func (s *resourceStore) ListKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.ListKeys()
}

func (s *resourceStore) Get(obj interface{}) (item interface{}, exists bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.Get(obj)
}

func (s *resourceStore) GetByKey(key string) (item interface{}, exists bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.GetByKey(key)
}

func (s *resourceStore) Replace(objs []interface{}, resVer string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(objs) == 0 {
		return fmt.Errorf("cannot handle situation when replace is called with empty slice")
	}

	accessor, err := meta.TypeAccessor(objs[0])
	if err != nil {
		return err
	}

	listKind := accessor.GetKind()

	var objsToSave []interface{}

	// TODO: if we receive an empty list, what do we do?
	kinds := s.Indexer.ListIndexFuncValues("ofKind")
	for _, kind := range kinds {
		if kind == listKind {
			continue
		}

		objsOfKind, err := s.Indexer.ByIndex("ofKind", kind)
		if err != nil {
			return err
		}
		objsToSave = append(objsToSave, objsOfKind...)
	}
	objsToDelete, err := s.Indexer.ByIndex("ofKind", listKind)

	s.Indexer.Replace(objsToSave, resVer)
	//s.Indexer.Replace([]interface{}{}, resVer)
	for _, obj := range objs {
		switch r := obj.(type) {
		case *kapi.Namespace, *kapi.Service, *kapi.Pod, *kapi.ReplicationController:
			if err := s.AddOrModify(r); err != nil {
				return err
			}
		default:
			glog.Errorf("Object was not recognized")
		}
	}

	for _, obj := range objsToDelete {
		_, exists, err := s.Indexer.Get(obj)
		if err != nil {
			return err
		}
		if !exists {
			// do what stopResource does
			s.Indexer.Add(obj)
			_, nsexists, err := s.Indexer.Get(obj.(*ResourceObject).Namespace)
			if err != nil {
				return err
			}
			if nsexists {
				s.stopResource(obj.(*ResourceObject))
			} else {
				s.Indexer.Delete(obj)
			}
		}
	}

	return nil
}

func (s *resourceStore) Resync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Indexer.Resync()
}

func (s *resourceStore) ByIndex(indexName, indexKey string) ([]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.ByIndex(indexName, indexKey)
}

func (s *resourceStore) Index(indexName string, obj interface{}) ([]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.Index(indexName, obj)
}

func (s *resourceStore) ListIndexFuncValues(indexName string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.ListIndexFuncValues(indexName)
}

func (s *resourceStore) GetIndexers() kcache.Indexers {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.GetIndexers()
}

func (s *resourceStore) AddIndexers(newIndexers kcache.Indexers) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.AddIndexers(newIndexers)
}

func resourceKey(obj interface{}) (string, error) {
	resObj := obj.(*ResourceObject)
	return string(resObj.UID), nil
}

// NewResourceStore creates an Indexer store with the given key function
func NewResourceStore(exclude map[string]bool, osClient osclient.Interface, kubeClient kclient.Interface) *resourceStore {
	store := &resourceStore{
		Indexer: kcache.NewIndexer(resourceKey, kcache.Indexers{
			"byNamespace":        indexResourceByNamespace,
			"byNamespaceAndKind": indexResourceByNamespaceAndKind,
			"getProject":         getProjectResource,
			"ofKind":             getAllResourcesOfKind,
			"rcByDC":             getRCByDC,
		}),
		Exclude:    exclude,
		OsClient:   osClient,
		KubeClient: kubeClient,
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store
}

type RunningTime struct {
	Start time.Time
	End   time.Time
}

// resourceStore methods
// Adds a new runningtime start value to a resource object
func (s *resourceStore) startResource(r *ResourceObject) {
	UID := string(r.UID)
	if !s.ResourceInCache(UID) {
		s.Indexer.Add(r)
	}

	// Make sure object was successfully created before working on it
	obj, exists, err := s.GetResourceByKey(UID)
	if err != nil {
		glog.Errorf("Error: %v", err)
	}

	if exists {
		res, err := kapi.Scheme.DeepCopy(obj)
		if err != nil {
			glog.Errorf("Couldn't copy resource from cache: %v", err)
			return
		}
		resource := res.(*ResourceObject)
		resource.Kind = r.Kind
		s.Indexer.Update(resource)
		if !resource.IsStarted() {
			runTime := &RunningTime{
				Start: time.Now(),
			}
			resource.RunningTimes = append(resource.RunningTimes, runTime)
			s.Indexer.Update(resource)
		}
	} else {
		glog.Errorf("Error starting resource: could not find resource %s %s\n", r.Name, UID)
		return
	}
}

// Adds an end time to a resource object
func (s *resourceStore) stopResource(r *ResourceObject) {
	var stopTime time.Time
	resourceTime := r.DeletionTimestamp
	UID := string(r.UID)

	// See if we already have a reference to this resource in cache
	if s.ResourceInCache(UID) {
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

	obj, exists, err := s.GetResourceByKey(UID)
	if err != nil {
		glog.Errorf("Error: %v", err)
	}
	if exists {
		res, err := kapi.Scheme.DeepCopy(obj)
		if err != nil {
			glog.Errorf("Couldn't copy resource from cache: %v", err)
			return
		}
		resource := res.(*ResourceObject)
		runTimeCount := len(resource.RunningTimes)

		if resource.IsStarted() {
			resource.RunningTimes[runTimeCount-1].End = stopTime
			if err := s.Indexer.Update(resource); err != nil {
				glog.Errorf("Error: %s", err)
			}
		}
	} else {
		glog.Errorf("Did not find resource %s %s\n", r.Name, UID)
	}
}

// Checks to see if a resource is stopped in our cache
// Ie, the resource's most recent RunningTime has a specified End time
func (r *ResourceObject) IsStopped() bool {
	runtimes := len(r.RunningTimes)
	if runtimes == 0 {
		return true
	} else {
		return !(r.RunningTimes[runtimes-1].End.IsZero())
	}
}

func (r *ResourceObject) IsStarted() bool {
	return !r.IsStopped()
}

func (r *ResourceObject) GetResourceRuntime(period time.Duration) (time.Duration, bool) {
	var total time.Duration
	count := len(r.RunningTimes) - 1
	outsidePeriod := 0

	for i := count; i >= 0; i-- {
		if i == count && r.IsStarted() {
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

func (s *resourceStore) ResourceInCache(UID string) bool {
	_, exists, err := s.Indexer.GetByKey(UID)
	if err != nil {
		return false
	}
	return exists
}

func (s *resourceStore) GetResourceByKey(UID string) (*ResourceObject, bool, error) {
	obj, exists, err := s.Indexer.GetByKey(UID)
	if err != nil {
		return nil, false, fmt.Errorf("Error getting resource by key: %v", err)
	}
	if exists {
		resource := obj.(*ResourceObject)
		return resource, true, nil
	}
	return nil, false, nil
}

func (s *resourceStore) NewResourceFromPod(pod *kapi.Pod) *ResourceObject {
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

func (s *resourceStore) NewResourceFromRC(rc *kapi.ReplicationController) *ResourceObject {
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

func (s *resourceStore) NewResourceFromService(svc *kapi.Service) *ResourceObject {
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

func (s *resourceStore) NewResourceFromProject(namespace *kapi.Namespace) *ResourceObject {
	resource := &ResourceObject{
		UID:              types.UID(namespace.Name),
		Name:             namespace.Name,
		Namespace:        namespace.Name,
		Kind:             ProjectKind,
		LastSleepTime:    time.Time{},
		ProjectSortIndex: 0.0,
		IsAsleep:         false,
	}

	// Parse any LastSleepTime annotation on the namespace
	if namespace.ObjectMeta.Annotations != nil {
		if namespace.ObjectMeta.Annotations[LastSleepTimeAnnotation] != "" {
			glog.V(2).Infof("Caching previously-set LastSleepTime for project %s: %+v", namespace.Name, namespace.ObjectMeta.Annotations[LastSleepTimeAnnotation])
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", namespace.ObjectMeta.Annotations[LastSleepTimeAnnotation])
			if err != nil {
				parsedTime = s.getParsedTimeFromQuotaCreation(namespace)
				glog.Errorf("Error parsing project LastSleepTime annotation on namespace: %v", err)
			}
			resource.LastSleepTime = parsedTime
			if !parsedTime.IsZero() {
				resource.IsAsleep = true
			}
		}
	}
	return resource
}

func (s *resourceStore) getParsedTimeFromQuotaCreation(namespace *kapi.Namespace) time.Time {
	// Try to see if a quota exists and if so, get sleeptime from there
	// If not, delete the annotation from the namespace object
	quotaInterface := s.KubeClient.ResourceQuotas(namespace.Name)
	quota, err := quotaInterface.Get(ProjectSleepQuotaName)
	exists := true
	if err != nil {
		if kerrors.IsNotFound(err) {
			exists = false
		} else {
			glog.Errorf("Error getting project resource quota: %v", err)
			return time.Time{}
		}
	}

	if exists {
		parsedTime, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", quota.ObjectMeta.CreationTimestamp.String())
		if err != nil {
			glog.Errorf("Error parsing quota creationtimestamp: %v", err)
		}
		return parsedTime
	} else {
		copy, err := kapi.Scheme.DeepCopy(namespace)
		if err != nil {
			glog.Errorf("Error copying project: %v", err)
		}
		newNamespace := copy.(*kapi.Namespace)
		delete(newNamespace.Annotations, LastSleepTimeAnnotation)
		_, err = s.KubeClient.Namespaces().Update(newNamespace)
		if err != nil {
			glog.Errorf("Error deleting project LastSleepTime: %v", err)
		}
	}
	return time.Time{}
}
