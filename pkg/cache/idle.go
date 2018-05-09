package cache

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/golang/glog"

	appsv1 "github.com/openshift/api/apps/v1"
	kappsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// These aren't in openshift/client-go, and we don't want to pull in
// origin just for these, so we've copied them over until they're replaced
// with actual types.
const (
	// IdledAtAnnotation indicates that a given object (endpoints or scalable object))
	// is currently idled (and the time at which it was idled)
	IdledAtAnnotation = "idling.alpha.openshift.io/idled-at"

	// UnidleTargetAnnotation contains the references and former scales for the scalable
	// objects associated with the idled endpoints
	UnidleTargetAnnotation = "idling.alpha.openshift.io/unidle-targets"

	// PreviousScaleAnnotation contains the previous scale of a scalable object
	PreviousScaleAnnotation = "idling.alpha.openshift.io/previous-scale"

	// ProjectLastSleepTime contains the time the project was put to sleep(resource quota 'force-sleep' placed in namesace)
	ProjectLastSleepTime  = "openshift.io/last-sleep-time"
)

// ControllerScaleReference holds information to set UnidleTargetAnnotation
type ControllerScaleReference struct {
	Name     string
	Kind     string
	Replicas int32
}

// ScaleAllScalableObjectsInNamespace places scale of 0 to all scalables
func (rs *ResourceStore) ScaleAllScalableObjectsInNamespace(namespace string) error {
	failed := false
	objCgrMap, err := rs.GetScalableObjRefsInProject(namespace)
	if err != nil {
		return err
	}
	for cgr, ref := range objCgrMap {
		_, currScaleObj, err := rs.GetObjectWithScale(namespace, cgr)
		if err != nil {
			failed = true
			glog.Errorf("%v", err)
		}
		if currScaleObj == nil {
			continue
		}
		currScale := currScaleObj.Spec.Replicas
		if currScale == 0 {
			// assume user manually scaled down, and ignore it.
			continue
		}
		newScale := &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ref.Name,
				Namespace: namespace,
			},
			Spec: autoscalingv1.ScaleSpec{Replicas: 0},
		}
		err = rs.UpdateObjectScale(namespace, cgr, ref, newScale)
		if err != nil {
			failed = true
			glog.Errorf("%v", err)
		}
	}
	if failed {
		return fmt.Errorf("failed to scale all scalable objects in project( %s )", namespace)
	}
	return nil
}

// Add idling PreviousScaleAnnotation to all scalable objects and endpoints in a namespace
// This function does one half of `oc idle`, with AddProjectIdledAtAnnotation adding the 2nd annotation
// The two functions should be used only if they are used together
func (rs *ResourceStore) AddProjectPreviousScaleAnnotation(namespace string) error {
	failed := false
	svcs, err := rs.ServiceList.Services(namespace).List(labels.Everything())
	if err != nil {
		return err
	}
	// Loop through all of the services in a namespace
	for _, svc := range svcs {
		glog.V(2).Infof("Adding previous scale annotation to service( %s )", svc.Name)
		err = rs.AnnotateService(svc, time.Time{}, namespace, PreviousScaleAnnotation)
		if err != nil {
			glog.Errorf("Project( %s ) Service( %s ): %s", namespace, svc.Name, err)
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("Failed to add previous scale annotation to all project( %s )services", namespace)
	}
	return nil
}

// Add idling IdledAtAnnotation to all services in a namespace
// This should only be called after calling AddProjectPreviousScaleAnnotation(), as it depends on the
// service's ScaleRefs annotation to determine which controllers belong to a service
func (rs *ResourceStore) AddProjectIdledAtAnnotation(namespace string, nowTime time.Time, sleepPeriod time.Duration) error {
	failed := false
	svcs, err := rs.ServiceList.Services(namespace).List(labels.Everything())
	if err != nil {
		return err
	}

	for _, svc := range svcs {
		err = rs.AnnotateService(svc, nowTime, namespace, IdledAtAnnotation)
		if err != nil {
			glog.Errorf("Project( %s ) Service( %s ): %s", namespace, svc.Name, err)
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("Failed to add idled-at annotation to all project( %s )services", namespace)
	}
	return nil
}

// AnnotateService adds the requested idling annotations to a service and all scalable objects and endpoints in that service
func (rs *ResourceStore) AnnotateService(svc *corev1.Service, nowTime time.Time, namespace, annotation string) error {
	endpointInterface := rs.KubeClient.CoreV1().Endpoints(namespace)
	endpoint, err := endpointInterface.Get(svc.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Error getting endpoint in namespace( %s ): %s", namespace, err)
	}
	epCopy := endpoint.DeepCopy()
	if epCopy.Annotations == nil {
		epCopy.Annotations = make(map[string]string)
	}
	objRefMap, err := rs.GetScalableObjRefsInProject(namespace)
	if err != nil {
		return err
	}
	scaleRefMap := make(map[*corev1.ObjectReference]*ControllerScaleReference)
	for cgr, ref := range objRefMap {
		// TODO: Limitation of oc idle and auto-idler: Cannot handle 2 services sharing a single RC.
		// Projects with 2 services sharing an RC will have unpredictable behavior of idling/auto-idling
		// This needs to be fixed in oc idle code.  Will document with auto-idling documentation for now.
		if annotation == PreviousScaleAnnotation {
			isAsleep, err := rs.IsAsleep(namespace)
			if err != nil {
				return err
			}
			// Only add annotations if there are pods associated with the endpoint.
			// If endpoint subsets len is 0, that means there is no pod for the endpoint
			// and the endpoint may already be idled.
			if !isAsleep && len(endpoint.Subsets) == 0 {
				//glog.V(0).Infof("GOT HERE, Endpoint.Subsets == 0")
				return nil
			}
			// ControllerScaleReference == Name, Kind, Replicas
			ctrlScaleRef, err := rs.annotateScalable(ref, cgr, nowTime, annotation, isAsleep, namespace)
			if err != nil {
				return err
			}
			scaleRefMap[ref] = ctrlScaleRef

			var endpointScaleRefs []*ControllerScaleReference
			for _, scaleRef := range scaleRefMap {
				endpointScaleRefs = append(endpointScaleRefs, scaleRef)
			}
			scaleRefsBytes, err := json.Marshal(endpointScaleRefs)
			if err != nil {
				return err
			}
			// Add the scalable resources annotation to the service (endpoint)
			epCopy.Annotations[UnidleTargetAnnotation] = string(scaleRefsBytes)
			_, err = endpointInterface.Update(epCopy)
			if err != nil {
				return err
			}
			return nil
		}
		var scaleRefs []ControllerScaleReference
		if annotation == IdledAtAnnotation {
			endpointInterface := rs.KubeClient.CoreV1().Endpoints(namespace)
			endpoint, err := endpointInterface.Get(svc.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("Error getting endpoint in namespace( %s ): %s", namespace, err)
			}
			epCopy := endpoint.DeepCopy()
			if epCopy.Annotations == nil {
				epCopy.Annotations = make(map[string]string)
			}

			// Add the annotation to the endpoint (service) and use the endpoints ScaleRef annotation to find
			// which controllers need to be annotated
			epCopy.Annotations[IdledAtAnnotation] = nowTime.Format(time.RFC3339)
			if _, ok := epCopy.Annotations[UnidleTargetAnnotation]; !ok {
				return fmt.Errorf("endpoint( %s )should have UnidleTargetAnnotation but none was found", epCopy.Name)
			}
			scaleRefsStr := epCopy.Annotations[UnidleTargetAnnotation]
			err = json.Unmarshal([]byte(scaleRefsStr), &scaleRefs)
			if err != nil {
				return err
			}
			_, err = endpointInterface.Update(epCopy)
			if err != nil {
				return err
			}
			isAsleep, err := rs.IsAsleep(namespace)
			if err != nil {
				return err
			}
			// Annotate the scalable
			for _, scaleRef := range scaleRefs {
				ref := &corev1.ObjectReference{
					Name:      scaleRef.Name,
					Kind:      scaleRef.Kind,
					Namespace: svc.Namespace,
				}
				_, err = rs.annotateScalable(ref, cgr, nowTime, annotation, isAsleep, namespace)
				if err != nil {
					return err
				}
			}
			// If project is currently being idled via force-sleeper upon wakeup, then we have to reset the dead pod in ns runtime annotation
			if isAsleep {
				proj, err := rs.ProjectList.Get(namespace)
				projectCopy := proj.DeepCopy()
				delete(projectCopy.Annotations, ProjectDeadPodsRuntimeAnnotation)
				// This should already be done, but just in case..
				projectCopy.Annotations[ProjectLastSleepTime] = time.Time{}.String()
				_, err = rs.KubeClient.CoreV1().Namespaces().Update(projectCopy)
				if err != nil {
					return err
				}

			}
		}
	}
	return nil
}

// annotateScalable adds idling annotations to all scalable objects in namespace based on the `annotation` parameter
func (rs *ResourceStore) annotateScalable(ref *corev1.ObjectReference, cgr CrossGroupObjectReference, nowTime time.Time, annotation string, isAsleep bool, namespace string) (*ControllerScaleReference, error) {
	var replicas int32
	// TODO: Use dynamic client here instead of switch...
	switch ref.Kind {
	case DCKind:
		dcInterface := rs.OSClient.AppsV1().DeploymentConfigs(namespace)
		dc, err := dcInterface.Get(ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		replicas = dc.Spec.Replicas
	case RCKind:
		rc, err := rs.Scalables.RCList.ReplicationControllers(namespace).Get(ref.Name)
		if err != nil {
			return nil, err
		}
		replicas = *rc.Spec.Replicas
	case RSKind:
		rs, err := rs.Scalables.RSList.ReplicaSets(namespace).Get(ref.Name)
		if err != nil {
			return nil, err
		}
		replicas = *rs.Spec.Replicas
	case DepKind:
		dep, err := rs.Scalables.DepList.Deployments(namespace).Get(ref.Name)
		if err != nil {
			return nil, err
		}
		replicas = *dep.Spec.Replicas
	case SSKind:
		ss, err := rs.Scalables.SSList.StatefulSets(namespace).Get(ref.Name)
		if err != nil {
			return nil, err
		}
		replicas = *ss.Spec.Replicas
	default:
		glog.V(2).Infof("unknown type %v", ref)
		return nil, fmt.Errorf("unknown type in annotateScalable: %v", ref)
	}

	scalableObj, _, err := rs.GetObjectWithScale(namespace, cgr)
	if err != nil {
		return nil, err
	}
	objMeta, err := apimeta.Accessor(scalableObj)
	if err != nil {
		return nil, err
	}
	scalableName := objMeta.GetName()
	switch annotation {
	case IdledAtAnnotation:
		annotateMap := make(map[string]string)
		annotateMap[IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		objMeta.SetAnnotations(annotateMap)
	case PreviousScaleAnnotation:
		currentAnnotations := objMeta.GetAnnotations()
		if _, ok := currentAnnotations[IdledAtAnnotation]; ok {
			if isAsleep {
				glog.V(2).Infof("Force-sleeper: Removing stale idled-at annotation in( %s ) project( %s )", scalableName, namespace)
			} else {
				glog.V(2).Infof("Auto-idler: Removing stale idled-at annotation in( %s ) project( %s )", scalableName, namespace)
			}
			delete(currentAnnotations, IdledAtAnnotation)
		}
		// This check of replicas ensures that a PreviousScaleAnnotation will not be set to 0
		// Don't replace a non-zero number with '0', this means controller is already idled,
		// so want to keep the PreviousScaleAnnotation as/is.
		if replicas == 0 && currentAnnotations[PreviousScaleAnnotation] != "0" {
			r, err := strconv.ParseInt(currentAnnotations[PreviousScaleAnnotation], 10, 32)
			if err != nil {
				return nil, err
			}
			replicas = int32(r)
		}
		annotateMap := make(map[string]string)
		annotateMap[PreviousScaleAnnotation] = fmt.Sprintf("%d", replicas)
		objMeta.SetAnnotations(annotateMap)
	}

	switch typedObj := scalableObj.(type) {
	case *appsv1.DeploymentConfig:
		dcInterface := rs.OSClient.AppsV1().DeploymentConfigs(namespace)
		_, err = dcInterface.Update(typedObj)
	case *corev1.ReplicationController:
		_, err := rs.KubeClient.CoreV1().ReplicationControllers(namespace).Update(typedObj)
		if err != nil {
			return nil, err
		}
	case *v1beta1.ReplicaSet:
		_, err := rs.KubeClient.ExtensionsV1beta1().ReplicaSets(namespace).Update(typedObj)
		if err != nil {
			return nil, err
		}
	case *kappsv1.StatefulSet:
		_, err := rs.KubeClient.AppsV1().StatefulSets(namespace).Update(typedObj)
		if err != nil {
			return nil, err
		}
	case *v1beta1.Deployment:
		_, err := rs.KubeClient.ExtensionsV1beta1().Deployments(namespace).Update(typedObj)
		if err != nil {
			return nil, err
		}
	default:
		glog.Errorf("typed Obj( %v )not recognized", typedObj)
		return nil, err
	}
	return &ControllerScaleReference{
		Name:     ref.Name,
		Kind:     ref.Kind,
		Replicas: replicas}, nil
}
