package cache

import (
	"fmt"
	"strconv"

	"github.com/golang/glog"
	//iclient "github.com/openshift/service-idler/pkg/client/clientset/versioned/typed/idling/v1alpha2"
	appsv1 "github.com/openshift/api/apps/v1"
	osclient "github.com/openshift/client-go/apps/clientset/versioned"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	informers "k8s.io/client-go/informers"
	kclient "k8s.io/client-go/kubernetes"
	listersappsv1 "k8s.io/client-go/listers/apps/v1"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/scale"
	kcache "k8s.io/client-go/tools/cache"
)

const (
	RCKind                           = "ReplicationController"
	DCKind                           = "DeploymentConfig"
	RSKind                           = "ReplicaSet"
	SSKind                           = "StatefulSet"
	DepKind                          = "Deployment"
	appsV1GroupName                  = "apps.openshift.io"
	ProjectDeadPodsRuntimeAnnotation = "openshift.io/project-dead-pods-runtime"
	HibernationLabel                 = "openshift.io/hibernate-include"
	ProjectIsAsleepAnnotation        = "openshift.io/project-is-asleep"
	ProjectSleepQuotaName            = "force-sleep"
	OpenShiftDCName                  = "openshift.io/deployment-config.name"
	BuildAnnotation                  = "openshift.io/build.name"
)

type ResourceStore struct {
	KubeClient  kclient.Interface
	OSClient    osclient.Interface
	PodList     listerscorev1.PodLister
	ProjectList listerscorev1.NamespaceLister
	ServiceList listerscorev1.ServiceLister
	Scalables   *ScalableStore
	ClientPool  dynamic.ClientPool
	ScaleClient scale.ScalesGetter
}

// ScalableStore holds scalable objects.
// TODO: Should be replaced once dynamic client is in use?
type ScalableStore struct {
	RCList  listerscorev1.ReplicationControllerLister
	RSList  listersappsv1.ReplicaSetLister
	DepList listersappsv1.DeploymentLister
	SSList  listersappsv1.StatefulSetLister
}

// NewResourceStore creates a ResourceStore for use by force-sleeper and auto-idler
func NewResourceStore(oclient osclient.Interface, kc kclient.Interface, dynamicClientPool dynamic.ClientPool, scaleClient scale.ScalesGetter, informerfactory informers.SharedInformerFactory, projectInformer kcache.SharedIndexInformer) *ResourceStore {
	informer := informerfactory.Core().V1().Pods()
	scalableStore := NewScalableStore(informerfactory)
	pl := listerscorev1.NewNamespaceLister(projectInformer.GetIndexer())
	resourceStore := &ResourceStore{
		KubeClient:  kc,
		OSClient:    oclient,
		PodList:     informer.Lister(),
		ProjectList: pl,
		ServiceList: informerfactory.Core().V1().Services().Lister(),
		Scalables:   scalableStore,
		ClientPool:  dynamicClientPool,
		ScaleClient: scaleClient,
	}
	informer.Informer().AddEventHandler(kcache.ResourceEventHandlerFuncs{
		DeleteFunc: func(podRaw interface{}) {
			pod := podRaw.(*corev1.Pod)
			if err := resourceStore.RecordDeletedPodRuntime(pod); err != nil {
				utilruntime.HandleError(err)
			}
		},
	})
	return resourceStore
}

func NewScalableStore(informerfactory informers.SharedInformerFactory) *ScalableStore {
	scalableStore := &ScalableStore{
		RCList:  informerfactory.Core().V1().ReplicationControllers().Lister(),
		RSList:  informerfactory.Apps().V1().ReplicaSets().Lister(),
		DepList: informerfactory.Apps().V1().Deployments().Lister(),
		SSList:  informerfactory.Apps().V1().StatefulSets().Lister(),
	}
	return scalableStore
}

// RecordDeletedPodRuntime keeps track of runtimes of dead pods per namespace as project annotation
func (rs *ResourceStore) RecordDeletedPodRuntime(pod *corev1.Pod) error {
	ns, err := rs.ProjectList.Get(pod.Namespace)
	// If deleted pod is in excluded namespace, ignore
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil
		} else {
			return err
		}
	}
	projCopy := ns.DeepCopy()
	var runtimeToAdd, thisPodRuntime, currentDeadPodRuntime float64
	if projCopy.Annotations == nil {
		projCopy.Annotations = make(map[string]string)
	}
	if _, ok := projCopy.Annotations[ProjectDeadPodsRuntimeAnnotation]; ok {
		currentDeadPodRuntimeStr := projCopy.ObjectMeta.Annotations[ProjectDeadPodsRuntimeAnnotation]
		currentDeadPodRuntime, err = strconv.ParseFloat(currentDeadPodRuntimeStr, 64)
		if err != nil {
			return err
		}
	} else {
		currentDeadPodRuntime = 0
	}

	// TODO: This is not exact.. is there a better way, to exclude pod startup time?
	// TODO: LOOK AT ContainerStateRunning.StartedAt.Time instead...
	if !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		thisPodRuntime = pod.ObjectMeta.DeletionTimestamp.Time.Sub(pod.Status.StartTime.Time).Seconds()
	}
	runtimeToAdd += thisPodRuntime
	totalRuntime := runtimeToAdd + currentDeadPodRuntime
	projCopy.ObjectMeta.Annotations[ProjectDeadPodsRuntimeAnnotation] = strconv.FormatFloat(totalRuntime, 'f', -1, 64)
	_, err = rs.KubeClient.CoreV1().Namespaces().Update(projCopy)
	if err != nil {
		return err
	}
	return nil
}

// GetPodsForService returns list of pods associated with given service
func (rs *ResourceStore) GetPodsForService(svc *corev1.Service, podList []*corev1.Pod) []*corev1.Pod {
	var podsWSvc []*corev1.Pod
	for _, pod := range podList {
		selector := labels.Set(svc.Spec.Selector).AsSelectorPreValidated()
		if selector.Matches(labels.Set(pod.Labels)) {
			podsWSvc = append(podsWSvc, pod)
		}
	}
	return podsWSvc
}

// TODO: use "scale" subresource to get this
func (rs *ResourceStore) ResourceIsScalable(resource, kind string) bool {
	scalable := false
	scalableResource := false
	scalableKind := false
	scalableResourceList := []string{"replicationcontrollers", "deployments", "deploymentconfigs", "statefulsets", "replicasets"}
	scalableKindList := []string{"ReplicationController", "Deployment", "DeploymentConfig", "StatefulSet", "ReplicaSet"}
	for _, v := range scalableResourceList {
		if v == resource {
			scalableResource = true
		}
	}
	for _, v := range scalableKindList {
		if v == kind {
			scalableKind = true
		}
	}
	if scalableResource == true && scalableKind == true {
		scalable = true
	}
	return scalable
}

// CrossGroupObjectReference allows for working with objects across groups
type CrossGroupObjectReference struct {
	// Kind of the referent; More info: http://releases.k8s.io/release-1.3/docs/devel/api-conventions.md#types-kinds"
	Kind string `json:"kind" protobuf:"bytes,1,opt,name=kind"`
	// Name of the referent; More info: http://releases.k8s.io/release-1.3/docs/user-guide/identifiers.md#names
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`
	// API version of the referent (deprecated, prefer usng Group instead)
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,3,opt,name=apiVersion"`
	// Group of the referent
	Group string `json:"group,omitempty" protobuf:"bytes,3,opt,name=group"`
	// Resource of the referent
	Resource string `json:"group,omitempty" protobuf:"bytes,3,opt,name=resource"`
}

// UpdateObjectScale updates the scale of an object and removes unidling annotations for objects of a known type.
// For objects of an unknown type, it scales the object using the scale subresource
// TODO: Use dynamic client instead of switch
func (rs *ResourceStore) UpdateObjectScale(namespace string, ref CrossGroupObjectReference, obj runtime.Object, scale *autoscalingv1.Scale) error {
	gr := schema.GroupResource{Group: ref.Group, Resource: ref.Kind}
	var err error
	if obj == nil {
		_, err = rs.ScaleClient.Scales(namespace).Update(gr, scale)
		return err
	}

	switch ref.Kind {
	case DCKind:
		dcInterface := rs.OSClient.AppsV1().DeploymentConfigs(namespace)
		dc, err := dcInterface.Get(ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		dcCopy := dc.DeepCopy()
		dcCopy.Spec.Replicas = scale.Spec.Replicas
		_, err = dcInterface.Update(dcCopy)
		if err != nil {
			return err
		}
	case RCKind:
		rc, err := rs.Scalables.RCList.ReplicationControllers(namespace).Get(ref.Name)
		if err != nil {
			return err
		}

		rc.Spec.Replicas = int32Ptr(scale.Spec.Replicas)
		_, err = rs.KubeClient.CoreV1().ReplicationControllers(namespace).Update(rc)
		if err != nil {
			return err
		}

	default:
		glog.V(2).Infof("unknown type %v", obj)
		_, err = rs.ScaleClient.Scales(namespace).Update(gr, scale)
	}

	return err
}

// GetObjectsWithScale is called whenever we need to collect or update an object's scale
// TODO: Use dynamic client instead of switch
func (rs *ResourceStore) GetObjectWithScale(namespace string, ref CrossGroupObjectReference) (runtime.Object, *autoscalingv1.Scale, error) {
	var obj runtime.Object
	var err error
	var scale *autoscalingv1.Scale

	switch {
	case ref.Kind == DCKind && (ref.Group == appsV1GroupName || ref.Group == ""):
		var dc *appsv1.DeploymentConfig
		dcInterface := rs.OSClient.AppsV1().DeploymentConfigs(namespace)
		dcList, err := dcInterface.List(metav1.ListOptions{})
		if err != nil {
			return nil, nil, err
		}
		dc = &dcList.Items[0]
		scale = &autoscalingv1.Scale{
			Spec: autoscalingv1.ScaleSpec{Replicas: dc.Spec.Replicas},
		}
		obj = dc
	case ref.Kind == RCKind && ref.Group == corev1.GroupName:
		var rc *corev1.ReplicationController
		rc, err = rs.Scalables.RCList.ReplicationControllers(namespace).Get(ref.Name)
		if err != nil {
			return nil, nil, err
		}
		scale = &autoscalingv1.Scale{
			Spec: autoscalingv1.ScaleSpec{Replicas: *rc.Spec.Replicas},
		}
		obj = rc
	default:
		gr := schema.GroupResource{Group: ref.Group, Resource: ref.Resource}
		scale, err = rs.ScaleClient.Scales(namespace).Get(gr, ref.Name)
		if err != nil {
			return nil, nil, err
		}
	}

	return obj, scale, err
}

// GetScalableObjRefsInNamespace returns a reference to scalable objects in a namespace
// Takes a namespace to find scalable objects and their parent object
// Then takes that list of parent objects and checks if there is another parent above them
// ex. pod -> RC -> DC, DC is the main parent object we want to scale
func (rs *ResourceStore) GetScalableObjRefsInProject(namespace string) (map[CrossGroupObjectReference]*corev1.ObjectReference, error) {
	resultMap := make(map[CrossGroupObjectReference]*corev1.ObjectReference)
	// APIResList is a []*metav1.APIResourceList
	APIResList, err := rs.KubeClient.Discovery().ServerPreferredNamespacedResources()
	if err != nil {
		return nil, err
	}
	failed := false
	for _, apiResList := range APIResList {
		apiResources := apiResList.APIResources
		for _, apiRes := range apiResources {
			if rs.ResourceIsScalable(apiRes.Name, apiRes.Kind) {
				gvstr := apiResList.GroupVersion
				gv, err := schema.ParseGroupVersion(gvstr)
				if err != nil {
					glog.Errorf("%v", err)
					failed = true
				}
				gvr := gv.WithResource(apiRes.Name)
				dynamicClientInterface, err := rs.ClientPool.ClientForGroupVersionResource(gvr)
				//dynamicClientInterface, err := rs.ClientPool.ClientForGroupVersionKind(scaleGVK)
				if err != nil {
					glog.Errorf("%v", err)
					failed = true
					continue
				}
				scalableObj, err := dynamicClientInterface.Resource(&apiRes, namespace).List(metav1.ListOptions{})
				if err != nil {
					glog.Errorf("%v", err)
					failed = true
					continue
				}
				err = apimeta.EachListItem(scalableObj, func(obj runtime.Object) error {
					objMeta, err := apimeta.Accessor(obj)
					if err != nil {
						glog.Errorf("%v", err)
						failed = true
					}
					ownerRef := objMeta.GetOwnerReferences()
					var ref metav1.OwnerReference
					var objRef *corev1.ObjectReference
					if len(ownerRef) != 0 {
						ref = ownerRef[0]
						objRef = &corev1.ObjectReference{
							Name:       ref.Name,
							Namespace:  namespace,
							Kind:       ref.Kind,
							APIVersion: gv.String(),
						}
					} else {
						objRef = &corev1.ObjectReference{
							Name:       objMeta.GetName(),
							Namespace:  namespace,
							Kind:       apiRes.Kind,
							APIVersion: gv.String(),
						}
					}
					isUnique := true
					for cgr, _ := range resultMap {
						// TODO: Check to make sure this is enough...
						if objRef.Name == cgr.Name {
							isUnique = false
						}
					}
					if objRef != nil && isUnique {
						cgr := CrossGroupObjectReference{
							Kind:       objRef.Kind,
							Name:       objRef.Name,
							APIVersion: objRef.APIVersion,
							Group:      gv.Group,
						}
						resultMap[cgr] = objRef
					}
					return nil
				})
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if len(resultMap) == 0 {
		glog.V(0).Infof("no scalable objects found in project( %s )", namespace)
	}
	if failed {
		return nil, fmt.Errorf("error finding scalable object references in namespace( %s )", namespace)
	}
	return resultMap, nil
}

// IsAsleep returns bool based on project IsAsleepAnnotation
func (rs *ResourceStore) IsAsleep(p string) (bool, error) {
	isAsleep := false
	project, err := rs.ProjectList.Get(p)
	if err != nil {
		return isAsleep, err
	}
	if val, ok := project.Annotations[ProjectIsAsleepAnnotation]; ok {
		if val == "true" {
			isAsleep = true
		}
	}
	return isAsleep, nil
}

// borrowed this convenience function from k8s.io/client-go/examples/create-update-delete-deployment
func int32Ptr(i int32) *int32 { return &i }
