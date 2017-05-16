package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	osclient "github.com/openshift/origin/pkg/client"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"
	deployapi "github.com/openshift/origin/pkg/deploy/api"

	"github.com/golang/glog"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/api/unversioned"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
	ctlresource "k8s.io/kubernetes/pkg/kubectl/resource"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/types"
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

type Cache struct {
	kcache.Indexer
	OsClient   osclient.Interface
	KubeClient kclient.Interface
	Factory    *clientcmd.Factory
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
	IsAsleep          bool
}

type RunningTime struct {
	Start time.Time
	End   time.Time
}

/*
 * ResourceObject methods
 */

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

/*
 * Cache methods
 */

func NewCache(osClient osclient.Interface, kubeClient kclient.Interface, f *clientcmd.Factory) *Cache {
	return &Cache{
		Indexer: kcache.NewIndexer(resourceKey, kcache.Indexers{
			"byNamespace":        indexResourceByNamespace,
			"byNamespaceAndKind": indexResourceByNamespaceAndKind,
			"getProject":         getProjectResource,
			"ofKind":             getAllResourcesOfKind,
			"rcByDC":             getRCByDC,
		}),
		OsClient:   osClient,
		KubeClient: kubeClient,
		Factory:    f,
	}
}

// Checks if we have a ResourceObject in cache for requested project
// If not, a new ResourceObject is created for that project
func (c *Cache) CreateProjectInCache(namespace string) {
	// Make sure we have a tracking resource for the project to monitor last sleep time
	proj, err := c.ByIndex("getProject", namespace)
	if err != nil {
		glog.Errorf("Error checking for project (%s) resources: %s", namespace, err)
	}
	if len(proj) == 0 {
		newResource := NewResourceFromProject(namespace)
		c.Add(newResource)
	}
}

// Checks if resource exists in project cache
func (c *Cache) ResourceInCache(UID string) bool {
	_, exists, err := c.GetByKey(UID)
	if err != nil {
		return false
	}
	return exists
}

func (c *Cache) GetResourceByKey(UID string) (*ResourceObject, bool, error) {
	obj, exists, err := c.GetByKey(UID)
	if err != nil {
		return nil, false, fmt.Errorf("error getting resource by key: %v", err)
	}
	if exists {
		resource := obj.(*ResourceObject)
		return resource, true, nil
	}
	return nil, false, nil
}

func NewResourceFromPod(pod *kapi.Pod) *ResourceObject {
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

func NewResourceFromRC(rc *kapi.ReplicationController) *ResourceObject {
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

func NewResourceFromService(svc *kapi.Service) *ResourceObject {
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

func NewResourceFromProject(namespace string) *ResourceObject {
	return &ResourceObject{
		UID:              types.UID(namespace),
		Name:             namespace,
		Namespace:        namespace,
		Kind:             ProjectKind,
		LastSleepTime:    time.Time{},
		ProjectSortIndex: 0.0,
		IsAsleep:         false,
	}
}

func NewResourceFromInterface(resource interface{}) *ResourceObject {
	switch r := resource.(type) {
	case *kapi.Pod:
		return NewResourceFromPod(r)
	case *kapi.ReplicationController:
		return NewResourceFromRC(r)
	case *kapi.Service:
		return NewResourceFromService(r)
	}
	return nil
}

func (c *Cache) GetProjectServices(namespace string) ([]interface{}, error) {
	namespaceAndKind := namespace + "/" + ServiceKind
	svcs, err := c.ByIndex("byNamespaceAndKind", namespaceAndKind)
	if err != nil {
		return nil, err
	}
	return svcs, nil
}

func (c *Cache) GetProjectPods(namespace string) ([]interface{}, error) {
	namespaceAndKind := namespace + "/" + PodKind
	pods, err := c.ByIndex("byNamespaceAndKind", namespaceAndKind)
	if err != nil {
		return nil, err
	}
	return pods, nil
}

func (c *Cache) GetProject(namespace string) (*ResourceObject, error) {
	projObj, err := c.ByIndex("getProject", namespace)
	if err != nil {
		return nil, fmt.Errorf("couldn't get project resources: %v", err)
	}
	if e, a := 1, len(projObj); e != a {
		return nil, fmt.Errorf("expected %d project named %s, got %d", e, namespace, a)
	}

	project := projObj[0].(*ResourceObject)
	return project, nil
}

// Takes in a list of locally cached Pod ResourceObjects and a Service ResourceObject
// Compares selector labels on the pods and services to check for any matches which link the two
func (c *Cache) GetPodsForService(svc *ResourceObject, podList []interface{}) map[string]runtime.Object {
	pods := make(map[string]runtime.Object)
	for _, obj := range podList {
		pod := obj.(*ResourceObject)
		for podKey, podVal := range pod.Labels {
			for svcKey, svcVal := range svc.Selectors {
				if _, exists := pods[pod.Name]; !exists && svcKey == podKey && svcVal == podVal {
					podRef, err := c.KubeClient.Pods(pod.Namespace).Get(pod.Name)
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

// Takes a list of Pods and looks at their parent controllers
// Then takes that list of parent controllers and checks if there is another parent above them
// ex. pod -> RC -> DC, DC is the main parent controller we want to idle
func (c *Cache) FindScalableResourcesForService(pods map[string]runtime.Object) (map[kapi.ObjectReference]struct{}, error) {
	immediateControllerRefs := make(map[kapi.ObjectReference]struct{})
	for _, pod := range pods {
		controllerRef, err := GetControllerRef(pod)
		if err != nil {
			return nil, err
		}
		immediateControllerRefs[*controllerRef] = struct{}{}
	}

	controllerRefs := make(map[kapi.ObjectReference]struct{})
	for controllerRef := range immediateControllerRefs {
		controller, err := GetController(controllerRef, c.Factory)
		if err != nil {
			return nil, err
		}

		if controller != nil {
			var parentControllerRef *kapi.ObjectReference
			parentControllerRef, err = GetControllerRef(controller)
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

// Returns an ObjectReference to the parent controller (RC/DC) for a resource
func GetControllerRef(obj runtime.Object) (*kapi.ObjectReference, error) {
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
func GetController(ref kapi.ObjectReference, f *clientcmd.Factory) (runtime.Object, error) {
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

func (c *Cache) GetAndCopyEndpoint(namespace, name string) (*kapi.Endpoints, error) {
	endpointInterface := c.KubeClient.Endpoints(namespace)
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

/*
 * Keying functions for Indexer
 */
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
