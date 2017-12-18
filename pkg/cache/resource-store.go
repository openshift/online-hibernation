package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"

	osclient "github.com/openshift/client-go/apps/clientset/versioned"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	kclient "k8s.io/client-go/kubernetes"
	kcache "k8s.io/client-go/tools/cache"
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
	LastSleepTime     time.Time
	ProjectSortIndex  float64
	DeletionTimestamp time.Time
	Selectors         labels.Selector
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

func (s *resourceStore) NewResourceFromInterface(resource interface{}) (*ResourceObject, error) {
	switch r := resource.(type) {
	case *corev1.Pod:
		return s.NewResourceFromPod(r), nil
	case *corev1.ReplicationController:
		return s.NewResourceFromRC(r), nil
	case *corev1.Service:
		return s.NewResourceFromService(r), nil
	case *corev1.Namespace:
		return s.NewResourceFromProject(r), nil
	case *v1beta1.ReplicaSet:
		newResource, err := s.NewResourceFromRS(r)
		if err != nil {
			return nil, err
		}
		return newResource, nil
	}
	return nil, fmt.Errorf("unknown resource of type %T", resource)
}

func (s *resourceStore) AddOrModify(obj interface{}) error {
	switch r := obj.(type) {
	case *corev1.Pod:
		resObj, err := s.NewResourceFromInterface(obj.(*corev1.Pod))
		if err != nil {
			return err
		}
		glog.V(3).Infof("Received ADD/MODIFY for pod: %s\n", resObj.Name)
		// If the pod is running, make sure it's started in cache
		// Otherwise, make sure it's stopped
		switch r.Status.Phase {
		case corev1.PodRunning:
			s.startResource(resObj)
		case corev1.PodSucceeded, corev1.PodFailed, corev1.PodUnknown:
			s.stopResource(resObj)
		}
	case *corev1.ReplicationController:
		resObj, err := s.NewResourceFromInterface(obj.(*corev1.ReplicationController))
		if err != nil {
			return err
		}
		glog.V(3).Infof("Received ADD/MODIFY for RC: %s\n", resObj.Name)
		// If RC has more than 0 active replicas, make sure it's started in cache
		if r.Status.Replicas > 0 {
			s.startResource(resObj)
		} else { // replicas == 0
			s.stopResource(resObj)
		}
	case *v1beta1.ReplicaSet:
		resObj, err := s.NewResourceFromInterface(obj.(*v1beta1.ReplicaSet))
		if err != nil {
			return err
		}
		glog.V(3).Infof("Received ADD/MODIFY for RS: %s\n", resObj.Name)
		// If RS has more than 0 active replicas, make sure it's started in cache
		if r.Status.Replicas > 0 {
			s.startResource(resObj)
		} else { // replicas == 0
			s.stopResource(resObj)
		}
	case *corev1.Service:
		resObj, err := s.NewResourceFromInterface(obj.(*corev1.Service))
		if err != nil {
			return err
		}
		glog.V(3).Infof("Received ADD/MODIFY for Service: %s\n", resObj.Name)
		UID := string(resObj.UID)
		obj, exists, err := s.GetResourceByKey(UID)
		if err != nil {
			return err
		}
		if exists {
			res, err := Scheme.DeepCopy(obj)
			if err != nil {
				return err
			}
			resource := res.(*ResourceObject)
			resource.Kind = ServiceKind
			return s.Indexer.Update(resource)
		} else {
			s.Indexer.Add(resObj)
		}
	case *corev1.Namespace:
		resObj, err := s.NewResourceFromInterface(obj.(*corev1.Namespace))
		if err != nil {
			return err
		}
		glog.V(3).Infof("Received ADD/MODIFY for Project: %s\n", r.Name)
		if r.Status.Phase == corev1.NamespaceActive {
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
			res, err := Scheme.DeepCopy(obj)
			if err != nil {
				return err
			}
			resource := res.(*ResourceObject)
			return s.Indexer.Update(resource)
		} else {
			if r.Status.Phase == corev1.NamespaceActive && !s.Exclude[r.Name] {
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
	case *corev1.Pod, *corev1.ReplicationController, *v1beta1.ReplicaSet:
		resObj, err := s.NewResourceFromInterface(r)
		if err != nil {
			return err
		}
		UID := string(resObj.UID)
		if s.ResourceInCache(UID) {
			s.stopResource(resObj)
		}
	case *corev1.Service:
		resObj, err := s.NewResourceFromInterface(r)
		if err != nil {
			return err
		}
		UID := string(resObj.UID)
		if s.ResourceInCache(UID) {
			s.Indexer.Delete(resObj)
		}
	case *corev1.Namespace:
		resObj, err := s.NewResourceFromInterface(r)
		if err != nil {
			return err
		}
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
	case *corev1.Namespace, *corev1.Service, *corev1.Pod, *corev1.ReplicationController, *v1beta1.ReplicaSet:
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
	case *corev1.Namespace, *corev1.Service, *corev1.Pod, *corev1.ReplicationController, *v1beta1.ReplicaSet:
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
	case *corev1.Namespace, *corev1.Service, *corev1.Pod, *corev1.ReplicationController, *v1beta1.ReplicaSet:
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
	case PodKind, RCKind, ServiceKind, RSKind:
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
	for _, obj := range objs {
		switch r := obj.(type) {
		case *corev1.Namespace, *corev1.Service, *corev1.Pod, *corev1.ReplicationController, *v1beta1.ReplicaSet:
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
		res, err := Scheme.DeepCopy(obj)
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
		res, err := Scheme.DeepCopy(obj)
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

func (s *resourceStore) NewResourceFromPod(pod *corev1.Pod) *ResourceObject {
	terminating := false
	if (pod.Spec.RestartPolicy != corev1.RestartPolicyAlways) && (pod.Spec.ActiveDeadlineSeconds != nil) {
		terminating = true
	}
	resource := &ResourceObject{
		UID:             pod.GetUID(),
		Name:            pod.GetName(),
		Namespace:       pod.GetNamespace(),
		Kind:            PodKind,
		Terminating:     terminating,
		MemoryRequested: pod.Spec.Containers[0].Resources.Limits["memory"],
		RunningTimes:    make([]*RunningTime, 0),
		Labels:          pod.GetLabels(),
		Annotations:     pod.GetAnnotations(),
	}
	if pod.ObjectMeta.DeletionTimestamp.IsZero() {
		resource.DeletionTimestamp = time.Time{}
	} else {
		resource.DeletionTimestamp = pod.ObjectMeta.DeletionTimestamp.Time
	}

	return resource
}

func (s *resourceStore) NewResourceFromRC(rc *corev1.ReplicationController) *ResourceObject {
	resource := &ResourceObject{
		UID:             rc.GetUID(),
		Name:            rc.GetName(),
		Namespace:       rc.GetNamespace(),
		Kind:            RCKind,
		ResourceVersion: rc.GetResourceVersion(),
		RunningTimes:    make([]*RunningTime, 0),
		Selectors:       labels.SelectorFromSet(rc.Spec.Selector),
		Labels:          rc.GetLabels(),
	}

	if rc.ObjectMeta.DeletionTimestamp.IsZero() {
		resource.DeletionTimestamp = time.Time{}
	} else {
		resource.DeletionTimestamp = rc.ObjectMeta.DeletionTimestamp.Time
	}
	return resource
}

func (s *resourceStore) NewResourceFromRS(rs *v1beta1.ReplicaSet) (*ResourceObject, error) {
	selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
	if err != nil {
		return nil, err
	}
	resource := &ResourceObject{
		UID:             rs.GetUID(),
		Name:            rs.GetName(),
		Namespace:       rs.GetNamespace(),
		Kind:            RSKind,
		ResourceVersion: rs.GetResourceVersion(),
		RunningTimes:    make([]*RunningTime, 0),
		Selectors:       selector,
		Labels:          rs.GetLabels(),
	}

	if rs.ObjectMeta.DeletionTimestamp.IsZero() {
		resource.DeletionTimestamp = time.Time{}
	} else {
		resource.DeletionTimestamp = rs.ObjectMeta.DeletionTimestamp.Time
	}
	return resource, nil
}

func (s *resourceStore) NewResourceFromService(svc *corev1.Service) *ResourceObject {
	resource := &ResourceObject{
		UID:             svc.GetUID(),
		Name:            svc.GetName(),
		Namespace:       svc.GetNamespace(),
		Kind:            ServiceKind,
		ResourceVersion: svc.GetResourceVersion(),
		Selectors:       labels.SelectorFromSet(svc.Spec.Selector),
	}

	return resource
}

func (s *resourceStore) NewResourceFromProject(namespace *corev1.Namespace) *ResourceObject {
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

func (s *resourceStore) getParsedTimeFromQuotaCreation(namespace *corev1.Namespace) time.Time {
	// Try to see if a quota exists and if so, get sleeptime from there
	// If not, delete the annotation from the namespace object
	quotaInterface := s.KubeClient.CoreV1().ResourceQuotas(namespace.Name)
	quota, err := quotaInterface.Get(ProjectSleepQuotaName, metav1.GetOptions{})
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
		copy, err := Scheme.DeepCopy(namespace)
		if err != nil {
			glog.Errorf("Error copying project: %v", err)
		}
		newNamespace := copy.(*corev1.Namespace)
		delete(newNamespace.Annotations, LastSleepTimeAnnotation)
		_, err = s.KubeClient.CoreV1().Namespaces().Update(newNamespace)
		if err != nil {
			glog.Errorf("Error deleting project LastSleepTime: %v", err)
		}
	}
	return time.Time{}
}
