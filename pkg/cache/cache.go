package cache

import (
	"encoding/json"
	"errors"
	"fmt"

	osclient "github.com/openshift/origin/pkg/client"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"
	deployapi "github.com/openshift/origin/pkg/deploy/api"

	"github.com/golang/glog"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/api/unversioned"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
	ctlresource "k8s.io/kubernetes/pkg/kubectl/resource"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/watch"
)

const (
	PodKind                   = "Pod"
	RCKind                    = "ReplicationController"
	ServiceKind               = "Service"
	ProjectKind               = "Namespace"
	ComputeQuotaName          = "compute-resources"
	ComputeTimeboundQuotaName = "compute-resources-timebound"
	ProjectSleepQuotaName     = "force-sleep"
	OpenShiftDCName           = "openshift.io/deployment-config.name"
	BuildAnnotation           = "openshift.io/build.name"
)

type ResourceIndexer interface {
	kcache.Indexer
	UpdateResourceObject(obj *ResourceObject) error
	DeleteResourceObject(obj *ResourceObject) error
	AddResourceObject(obj *ResourceObject) error
}

type Cache struct {
	Indexer     ResourceIndexer
	OsClient    osclient.Interface
	KubeClient  kclient.Interface
	Factory     *clientcmd.Factory
	stopChannel <-chan struct{}
}

func NewCache(osClient osclient.Interface, kubeClient kclient.Interface, f *clientcmd.Factory, exclude map[string]bool) *Cache {
	return &Cache{
		Indexer:    NewResourceStore(exclude, osClient, kubeClient),
		OsClient:   osClient,
		KubeClient: kubeClient,
		Factory:    f,
	}
}

func (c *Cache) Run(stopChan <-chan struct{}) {
	c.stopChannel = stopChan

	//Call to watch for incoming events
	c.SetUpIndexer()
}

func (c *Cache) GetProjectServices(namespace string) ([]interface{}, error) {
	namespaceAndKind := namespace + "/" + ServiceKind
	svcs, err := c.Indexer.ByIndex("byNamespaceAndKind", namespaceAndKind)
	if err != nil {
		return nil, err
	}
	return svcs, nil
}

func (c *Cache) GetProjectPods(namespace string) ([]interface{}, error) {
	namespaceAndKind := namespace + "/" + PodKind
	pods, err := c.Indexer.ByIndex("byNamespaceAndKind", namespaceAndKind)
	if err != nil {
		return nil, err
	}
	return pods, nil
}

func (c *Cache) GetProject(namespace string) (*ResourceObject, error) {
	projObj, err := c.Indexer.ByIndex("getProject", namespace)
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
						// glog.Errorf?
						glog.V(3).Infof("Project( %s ): %s, continuing...\n", pod.Namespace, err)
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
				return nil, fmt.Errorf("Unable to load the creator of %s %q: %v", controllerRef.Kind, controllerRef.Name, err)
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
		return nil, errors.New(fmt.Sprintf("Error annotating endpoint in namespace %s: %s\n", namespace, err))
	}
	copy, err := kapi.Scheme.DeepCopy(endpoint)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error annotating endpoint in namespace %s: %s\n", namespace, err))
	}
	newEndpoint := copy.(*kapi.Endpoints)

	if newEndpoint.Annotations == nil {
		newEndpoint.Annotations = make(map[string]string)
	}
	return newEndpoint, nil
}

// Keying functions for Indexer
func indexResourceByNamespace(obj interface{}) ([]string, error) {
	object := obj.(*ResourceObject)
	return []string{object.Namespace}, nil
}

func getProjectResource(obj interface{}) ([]string, error) {
	object := obj.(*ResourceObject)
	if object.Kind == ProjectKind {
		return []string{object.Name}, nil
	}
	return []string{}, nil
}

func getRCByDC(obj interface{}) ([]string, error) {
	object := obj.(*ResourceObject)
	if object.Kind == RCKind {
		return []string{object.DeploymentConfig}, nil
	}
	return []string{}, nil
}

func getAllResourcesOfKind(obj interface{}) ([]string, error) {
	object := obj.(*ResourceObject)
	return []string{object.Kind}, nil
}

func indexResourceByNamespaceAndKind(obj interface{}) ([]string, error) {
	object := obj.(*ResourceObject)
	fullName := object.Namespace + "/" + object.Kind
	return []string{fullName}, nil
}

func (c *Cache) SetUpIndexer() {

	podLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return c.KubeClient.Pods(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return c.KubeClient.Pods(kapi.NamespaceAll).Watch(options)
		},
	}
	kcache.NewReflector(podLW, &kapi.Pod{}, c.Indexer, 0).Run()

	rcLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return c.KubeClient.ReplicationControllers(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return c.KubeClient.ReplicationControllers(kapi.NamespaceAll).Watch(options)
		},
	}
	kcache.NewReflector(rcLW, &kapi.ReplicationController{}, c.Indexer, 0).Run()

	svcLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return c.KubeClient.Services(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return c.KubeClient.Services(kapi.NamespaceAll).Watch(options)
		},
	}
	kcache.NewReflector(svcLW, &kapi.Service{}, c.Indexer, 0).Run()

	projLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return c.KubeClient.Namespaces().List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return c.KubeClient.Namespaces().Watch(options)
		},
	}
	kcache.NewReflector(projLW, &kapi.Namespace{}, c.Indexer, 0).Run()
}
