package cache

import (
	"fmt"

	"github.com/golang/glog"
	"github.com/openshift/api/apps/v1"
	osclient "github.com/openshift/client-go/apps/clientset/versioned"
	appsv1 "github.com/openshift/client-go/apps/clientset/versioned/typed/apps/v1"
	"k8s.io/api/extensions/v1beta1"
	extkclientv1beta1 "k8s.io/client-go/kubernetes/typed/extensions/v1beta1"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	kclient "k8s.io/client-go/kubernetes"
	kclientv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	kcache "k8s.io/client-go/tools/cache"

	appsscheme "github.com/openshift/client-go/apps/clientset/versioned/scheme"
	serializer "k8s.io/apimachinery/pkg/runtime/serializer"
	kscheme "k8s.io/client-go/kubernetes/scheme"
)

var Scheme = runtime.NewScheme()
var Codecs = serializer.NewCodecFactory(Scheme)

func init() {
	kscheme.AddToScheme(Scheme)
	appsscheme.AddToScheme(Scheme)
}

const (
	PodKind               = "Pod"
	RCKind                = "ReplicationController"
	DCKind                = "DeploymentConfig"
	RSKind                = "ReplicaSet"
	DepKind               = "Deployment"
	ServiceKind           = "Service"
	ProjectKind           = "Namespace"
	ProjectSleepQuotaName = "force-sleep"
	OpenShiftDCName       = "openshift.io/deployment-config.name"
	BuildAnnotation       = "openshift.io/build.name"
)

type ResourceIndexer interface {
	kcache.Indexer
	UpdateResourceObject(obj *ResourceObject) error
	DeleteResourceObject(obj *ResourceObject) error
	AddResourceObject(obj *ResourceObject) error
}

type Cache struct {
	Indexer    ResourceIndexer
	OsClient   osclient.Interface
	KubeClient kclient.Interface
	Config     *restclient.Config
	RESTMapper apimeta.RESTMapper
	stopChan   <-chan struct{}
}

func NewCache(osClient osclient.Interface, kubeClient kclient.Interface, config *restclient.Config, mapper apimeta.RESTMapper, exclude map[string]bool) *Cache {
	return &Cache{
		Indexer:    NewResourceStore(exclude, osClient, kubeClient),
		OsClient:   osClient,
		KubeClient: kubeClient,
		Config:     config,
		RESTMapper: mapper,
	}
}

func (c *Cache) Run(stopChan <-chan struct{}) {
	c.stopChan = stopChan

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
// Pod label must match service selector to link the two
// TODO: Best way to find pods for service?
func (c *Cache) GetPodsForService(svc *ResourceObject, podList []interface{}) map[string]runtime.Object {
	pods := make(map[string]runtime.Object)
	for _, obj := range podList {
		pod := obj.(*ResourceObject)
		if svc.Selectors.Matches(labels.Set(pod.Labels)) {
			podRef, err := c.KubeClient.CoreV1().Pods(pod.Namespace).Get(pod.Name, metav1.GetOptions{})
			if err != nil {
				// This may fail when a pod has been deleted but not yet pruned from cache
				// glog.Errorf?
				glog.V(3).Infof("Project( %s ): %s, continuing...\n", pod.Namespace, err)
				continue
			}
			pods[pod.Name] = podRef
		}
	}
	return pods
}

// Takes a list of Pods and looks at their parent controllers
// Then takes that list of parent controllers and checks if there is another parent above them
// ex. pod -> RC -> DC, DC is the main parent controller we want to idle
func (c *Cache) FindScalableResourcesForService(pods map[string]runtime.Object) (map[corev1.ObjectReference]struct{}, error) {
	immediateControllerRefs := make(map[corev1.ObjectReference]struct{})
	for _, pod := range pods {
		controllerRef, err := GetControllerRef(pod)
		if err != nil {
			return nil, err
		}
		immediateControllerRefs[*controllerRef] = struct{}{}
	}

	controllerRefs := make(map[corev1.ObjectReference]struct{})
	for controllerRef := range immediateControllerRefs {
		controller, err := GetController(controllerRef, c.RESTMapper, c.Config)
		if err != nil {
			return nil, err
		}

		if controller != nil {
			var parentControllerRef *corev1.ObjectReference
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

// Returns an ObjectReference to the parent controller (RC/DC/RS/Deployment) for a resource
func GetControllerRef(obj runtime.Object) (*corev1.ObjectReference, error) {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	ownerRef := objMeta.GetOwnerReferences()
	var ref metav1.OwnerReference
	if len(ownerRef) != 0 {
		ref = ownerRef[0]
		return &corev1.ObjectReference{
			Name:      ref.Name,
			Namespace: objMeta.GetNamespace(),
			Kind:      ref.Kind,
		}, nil
	} else {
		return nil, nil
	}
}

// Returns a generic runtime.Object for a controller
func GetController(ref corev1.ObjectReference, restMapper apimeta.RESTMapper, restConfig *restclient.Config) (runtime.Object, error) {
	// copy the config
	newConfig := *restConfig
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil, err
	}
	switch ref.Kind {
	case DCKind:
		gv = v1.SchemeGroupVersion
	case DepKind, RSKind:
		gv = v1beta1.SchemeGroupVersion
	}

	mapping, err := restMapper.RESTMapping(schema.GroupKind{Group: gv.Group, Kind: ref.Kind})
	if err != nil {
		return nil, err
	}
	newConfig.GroupVersion = &gv
	switch gv.Group {
	case corev1.GroupName:
		newConfig.APIPath = "/api"
	default:
		newConfig.APIPath = "/apis"
	}

	if ref.Kind == DCKind {
		oc := appsv1.NewForConfigOrDie(&newConfig)
		oclient := oc.RESTClient()
		req := oclient.Get().
			NamespaceIfScoped(ref.Namespace, mapping.Scope.Name() == apimeta.RESTScopeNameNamespace).
			Resource(mapping.Resource).
			Name(ref.Name).Do()

		result, err := req.Get()
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	if ref.Kind == DepKind || ref.Kind == RSKind {
		extkc := extkclientv1beta1.NewForConfigOrDie(&newConfig)
		extkcclient := extkc.RESTClient()
		req := extkcclient.Get().
			NamespaceIfScoped(ref.Namespace, mapping.Scope.Name() == apimeta.RESTScopeNameNamespace).
			Resource(mapping.Resource).
			Name(ref.Name).Do()

		result, err := req.Get()
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	kc := kclientv1.NewForConfigOrDie(&newConfig)
	client := kc.RESTClient()
	req := client.Get().
		NamespaceIfScoped(ref.Namespace, mapping.Scope.Name() == apimeta.RESTScopeNameNamespace).
		Resource(mapping.Resource).
		Name(ref.Name).Do()

	result, err := req.Get()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Cache) GetAndCopyEndpoint(namespace, name string) (*corev1.Endpoints, error) {
	endpointInterface := c.KubeClient.CoreV1().Endpoints(namespace)
	endpoint, err := endpointInterface.Get(name, metav1.GetOptions{})
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, fmt.Errorf("Error getting endpoint in namespace( %s ): %s", namespace, err)
		}
	}
	copy, err := Scheme.DeepCopy(endpoint)
	if err != nil {
		return nil, fmt.Errorf("Error copying endpoint in namespace( %s ): %s", namespace, err)
	}
	newEndpoint := copy.(*corev1.Endpoints)

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
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return c.KubeClient.CoreV1().Pods(metav1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.KubeClient.CoreV1().Pods(metav1.NamespaceAll).Watch(options)
		},
	}
	podr := kcache.NewReflector(podLW, &corev1.Pod{}, c.Indexer, 0)
	go podr.Run(c.stopChan)

	rcLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return c.KubeClient.CoreV1().ReplicationControllers(metav1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.KubeClient.CoreV1().ReplicationControllers(metav1.NamespaceAll).Watch(options)
		},
	}
	rcr := kcache.NewReflector(rcLW, &corev1.ReplicationController{}, c.Indexer, 0)
	go rcr.Run(c.stopChan)

	rsLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return c.KubeClient.ExtensionsV1beta1().ReplicaSets(metav1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.KubeClient.ExtensionsV1beta1().ReplicaSets(metav1.NamespaceAll).Watch(options)
		},
	}
	rsr := kcache.NewReflector(rsLW, &v1beta1.ReplicaSet{}, c.Indexer, 0)
	go rsr.Run(c.stopChan)

	svcLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return c.KubeClient.CoreV1().Services(metav1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.KubeClient.CoreV1().Services(metav1.NamespaceAll).Watch(options)
		},
	}
	svcr := kcache.NewReflector(svcLW, &corev1.Service{}, c.Indexer, 0)
	go svcr.Run(c.stopChan)

	projLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return c.KubeClient.CoreV1().Namespaces().List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.KubeClient.CoreV1().Namespaces().Watch(options)
		},
	}
	projr := kcache.NewReflector(projLW, &corev1.Namespace{}, c.Indexer, 0)
	go projr.Run(c.stopChan)
}
