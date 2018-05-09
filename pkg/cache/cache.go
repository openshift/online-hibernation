package cache

import (
	"fmt"
	"strconv"

	"github.com/golang/glog"
	svcidler "github.com/openshift/service-idler/pkg/apis/idling/v1alpha2"
	iclient "github.com/openshift/service-idler/pkg/client/clientset/versioned/typed/idling/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	informers "k8s.io/client-go/informers"
	kclient "k8s.io/client-go/kubernetes"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/scale"
	kcache "k8s.io/client-go/tools/cache"
)

const (
	// ProjectLastSleepTime contains the time the project was put to sleep(resource quota 'force-sleep' placed in namesace)
	ProjectLastSleepTime             = "openshift.io/last-sleep-time"
	ProjectDeadPodsRuntimeAnnotation = "openshift.io/project-dead-pods-runtime"
	HibernationLabel                 = "openshift.io/hibernate-include"
	HibernationIdler                 = "hibernation"
	ProjectSleepQuotaName            = "force-sleep"
	OpenShiftDCName                  = "openshift.io/deployment-config.name"
	BuildAnnotation                  = "openshift.io/build.name"
)

type ResourceStore struct {
	KubeClient        kclient.Interface
	PodList           listerscorev1.PodLister
	ProjectList       listerscorev1.NamespaceLister
	ServiceList       listerscorev1.ServiceLister
	ClientPool        dynamic.ClientPool
	IdlersClient      iclient.IdlersGetter
	ScaleKindResolver scale.ScaleKindResolver
}

// NewResourceStore creates a ResourceStore for use by force-sleeper and auto-idler
func NewResourceStore(kc kclient.Interface, dynamicClientPool dynamic.ClientPool, idlersClient iclient.IdlersGetter, scaleKindResolver scale.ScaleKindResolver, informerfactory informers.SharedInformerFactory, projectInformer kcache.SharedIndexInformer) *ResourceStore {
	informer := informerfactory.Core().V1().Pods()
	pl := listerscorev1.NewNamespaceLister(projectInformer.GetIndexer())
	resourceStore := &ResourceStore{
		KubeClient:        kc,
		PodList:           informer.Lister(),
		ProjectList:       pl,
		ServiceList:       informerfactory.Core().V1().Services().Lister(),
		ClientPool:        dynamicClientPool,
		IdlersClient:      idlersClient,
		ScaleKindResolver: scaleKindResolver,
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

// RecordDeletedPodRuntime keeps track of runtimes of dead pods per namespace as project annotation
func (rs *ResourceStore) RecordDeletedPodRuntime(pod *corev1.Pod) error {
	isAsleep, err := rs.IsAsleep(pod.Namespace)
	if err != nil {
		return err
	}
	// don't add deletedPodRuntimes if project is in force-sleep
	if isAsleep {
		return nil
	}
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

// ResourceIsScalable returns true if resource has "scale" subresource
func (rs *ResourceStore) ResourceIsScalable(gvr schema.GroupVersionResource) bool {
	_, err := rs.ScaleKindResolver.ScaleForResource(gvr)
	//TODO: log an err if it's not the 'could not find scale resource for this gvr' error
	if err != nil {
		return false
	}
	return true
}

// getTargetScalablesInProject gets info for svcidler.TargetScalables
// Takes a namespace to find scalable objects and their parent object
// Then takes that list of parent objects and checks if there is another parent above them
// ex. pod -> RC -> DC, DC is the main parent object we want to scale
func (rs *ResourceStore) getTargetScalablesInProject(namespace string) ([]svcidler.CrossGroupObjectReference, error) {
	var targetScalables []svcidler.CrossGroupObjectReference
	// APIResList is a []*metav1.APIResourceList
	APIResList, err := rs.KubeClient.Discovery().ServerPreferredNamespacedResources()
	if err != nil {
		return nil, err
	}
	quotableResources := discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"list", "watch", "get"}}, APIResList)
	quotableGroupVersionResources, err := discovery.GroupVersionResources(quotableResources)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse resources: %v", err)
	}
	failed := false
	for _, apiResList := range APIResList {
		apiResources := apiResList.APIResources
		for _, apiRes := range apiResources {
			gvstr := apiResList.GroupVersion
			gv, err := schema.ParseGroupVersion(gvstr)
			if err != nil {
				glog.Errorf("%v", err)
				failed = true
			}
			gvrPossible := gv.WithResource(apiRes.Name)
			if rs.ResourceIsScalable(gvrPossible) {
				for gvr, _ := range quotableGroupVersionResources {
					if gvrPossible == gvr {
						dynamicClientInterface, err := rs.ClientPool.ClientForGroupVersionResource(gvr)
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
							// Check if unique
							for _, cgr := range targetScalables {
								// TODO: Check to make sure this is enough...
								if objRef.Name == cgr.Name && objRef.Kind == cgr.Resource {
									continue
								}
							}
							if objRef != nil {
								cgr := svcidler.CrossGroupObjectReference{
									Name:     objRef.Name,
									Group:    gv.Group,
									Resource: objRef.Kind,
								}
								targetScalables = append(targetScalables, cgr)
							}
							return nil
						})
						if err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}
	if len(targetScalables) == 0 {
		glog.V(0).Infof("no scalable objects found in project( %s )", namespace)
	}
	if failed {
		return nil, fmt.Errorf("error finding scalable object references in namespace( %s )", namespace)
	}
	return targetScalables, nil
}

// GetIdlerTriggerServiceNames populates Idler IdlerSpec.TriggerServiceNames
func (rs *ResourceStore) GetIdlerTriggerServiceNames(namespace string) ([]string, error) {
	var triggerSvcName []string
	svcsInProj, err := rs.ServiceList.Services(namespace).List(labels.Everything())
	if err != nil {
		return []string{}, err
	}
	for _, svc := range svcsInProj {
		triggerSvcName = append(triggerSvcName, svc.Name)
	}
	return triggerSvcName, nil
}

// IsAsleep returns bool based on project IsAsleepAnnotation
func (rs *ResourceStore) IsAsleep(proj string) (bool, error) {
	quota, err := rs.KubeClient.CoreV1().ResourceQuotas(proj).Get(ProjectSleepQuotaName, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	// not sure we need this, needed in testing..
	if quota == nil {
		return false, nil
	}
	return true, nil
}

// CreateOrUpdateIdler creates an Idler named cache.HibernationIdler in given namespace.
// Note, if called by force-sleeper when placing project to sleep via resource quota,
// TriggerServiceNames is kept an empty array, until quota is removed, at which point, Idler will
// be updated with TriggerServiceNames.
func (rs *ResourceStore) CreateOrUpdateIdler(namespace string, forIdling bool) error {
	exists := false
	idler, err := rs.IdlersClient.Idlers(namespace).Get(HibernationIdler, metav1.GetOptions{})
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return err
		}
	}
	if idler != nil && idler.Name == HibernationIdler {
		exists = true
	}
	var triggerServiceNames []string
	// if !forIdling, the force-sleeper has called this.. so don't want to fill TriggerSvcNames yet, until
	// project is 'woken up' by removing force-sleep quota, then we'll populate the TriggerServiceNames
	if !forIdling {
		triggerServiceNames = []string{}
	}
	triggerServiceNames, err = rs.GetIdlerTriggerServiceNames(namespace)
	if err != nil {
		return err
	}
	targetScalables, err := rs.getTargetScalablesInProject(namespace)
	if !exists {
		newIdler := &svcidler.Idler{
			ObjectMeta: metav1.ObjectMeta{
				Name: HibernationIdler,
			},
			Spec: svcidler.IdlerSpec{
				WantIdle:            true,
				TargetScalables:     targetScalables,
				TriggerServiceNames: triggerServiceNames,
			},
			Status: svcidler.IdlerStatus{
				UnidledScales:        []svcidler.UnidleInfo{},
				InactiveServiceNames: []string{},
			},
		}
		_, err = rs.IdlersClient.Idlers(namespace).Create(newIdler)
		if err != nil {
			return err
		}
		return nil
	}
	newIdler := idler.DeepCopy()
	newIdler.Spec.WantIdle = true
	newIdler.Spec.TargetScalables = targetScalables
	newIdler.Spec.TriggerServiceNames = triggerServiceNames
	if len(idler.Status.UnidledScales) == 0 {
		newIdler.Status.UnidledScales = []svcidler.UnidleInfo{}
	}
	if len(idler.Status.InactiveServiceNames) == 0 {
		newIdler.Status.InactiveServiceNames = []string{}
	}
	_, err = rs.IdlersClient.Idlers(namespace).Update(newIdler)
	if err != nil {
		return err
	}
	return nil
}
