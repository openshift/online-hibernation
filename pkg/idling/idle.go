package idling

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"

	deployapi "github.com/openshift/origin/pkg/deploy/api"
	unidlingapi "github.com/openshift/origin/pkg/unidling/api"

	"github.com/golang/glog"

	kapi "k8s.io/kubernetes/pkg/api"
	kerrors "k8s.io/kubernetes/pkg/api/errors"
)

// These are just to provide the "FullIdleAnnotations" constant to be used as a parameter when idling
// The unidlingapi constants are copied over for consistency
const (
	IdledAtAnnotation       = unidlingapi.IdledAtAnnotation
	PreviousScaleAnnotation = unidlingapi.PreviousScaleAnnotation
	UnidleTargetAnnotation  = unidlingapi.UnidleTargetAnnotation
	FullIdleAnnotations     = "full"
)

type ControllerScaleReference struct {
	Kind     string
	Name     string
	Replicas int32
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
func IdleProjectServices(c *cache.Cache, namespace string) error {
	failed := false
	services, err := c.GetProjectServices(namespace)
	if err != nil {
		return err
	}
	nowTime := time.Now()
	// Loop through all of the services in a namespace
	for _, obj := range services {
		svc := obj.(*cache.ResourceObject)
		err = AnnotateService(c, svc, nowTime, namespace, FullIdleAnnotations)
		if err != nil {
			glog.Errorf("Error idling service: %s", err)
			failed = true
		}
		glog.V(0).Infof("Added service idled-at annotation( %s )", svc.Name)
	}

	if failed {
		return errors.New("Failed to idle all project services")
	}
	// TODO: What do we do with pods that have no service? If we scale them, they will remain
	// scaled down until a user manually scales them up again.. for now, only scale pods with
	// services...
	if len(services) != 0 {
		glog.V(2).Infof("Scaling DCs in project %s\n", namespace)
		err = ScaleProjectDCs(c, namespace)
		if err != nil {
			failed = true
			glog.Errorf("Error scaling DCs in project %s: %s\n", namespace, err)
		}

		glog.V(2).Infof("Scaling RCs in project %s\n", namespace)
		err = ScaleProjectRCs(c, namespace)
		if err != nil {
			failed = true
			glog.Errorf("Error scaling RCs in project %s: %s\n", namespace, err)
		}

		if failed {
			return fmt.Errorf("Error idling services in project %s\n", namespace)
		}
	} else {
		glog.V(2).Infof("Did not idle resources in project( %s ), no services found", namespace)
	}
	return nil
}

func ScaleProjectDCs(c *cache.Cache, namespace string) error {
	dcInterface := c.OsClient.DeploymentConfigs(namespace)
	dcList, err := dcInterface.List(kapi.ListOptions{})
	if err != nil {
		return err
	}

	failed := false
	for _, dc := range dcList.Items {
		// Scale down DC
		copy, err := kapi.Scheme.DeepCopy(dc)
		if err != nil {
			return err
		}
		newDC := copy.(deployapi.DeploymentConfig)
		newDC.Spec.Replicas = 0
		_, err = dcInterface.Update(&newDC)
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

func ScaleProjectRCs(c *cache.Cache, namespace string) error {
	failed := false
	// Scale RCs to 0
	rcInterface := c.KubeClient.ReplicationControllers(namespace)
	rcList, err := rcInterface.List(kapi.ListOptions{})
	if err != nil {
		return err
	}

	for _, thisRC := range rcList.Items {
		rcWDC, err := c.Indexer.ByIndex("dcByRC", thisRC.Name)
		if err != nil {
			return err
		}
		if len(rcWDC) != 0 {
			copy, err := kapi.Scheme.DeepCopy(thisRC)
			if err != nil {
				return err
			}
			newRC := copy.(kapi.ReplicationController)
			newRC.Spec.Replicas = 0
			_, err = rcInterface.Update(&newRC)
			if err != nil {
				if kerrors.IsNotFound(err) {
					continue
				} else {
					glog.Errorf("Error scaling RC in namespace %s: %s\n", namespace, err)
					failed = true
				}
			}
		}
	}
	if failed {
		return errors.New("Failed to scale all project RCs")
	}
	return nil
}

func DeleteProjectPods(c *cache.Cache, namespace string) error {
	// Delete running pods.
	podInterface := c.KubeClient.Pods(namespace)
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

// Add idling PreviousScaleAnnotation to all controllers in a namespace
// This function does one half of `oc idle`, with AddProjectIdledAtAnnotation adding the 2nd annotation
// The two functions should be used only if they are used together
func AddProjectPreviousScaleAnnotation(c *cache.Cache, namespace string) error {
	failed := false
	svcs, err := c.GetProjectServices(namespace)
	if err != nil {
		return err
	}

	// Loop through all of the services in a namespace
	for _, obj := range svcs {
		svc := obj.(*cache.ResourceObject)
		err = AnnotateService(c, svc, time.Time{}, namespace, PreviousScaleAnnotation)
		if err != nil {
			glog.Errorf("Error idling service: %s", err)
			failed = true
		}
	}
	if failed {
		return errors.New("Failed to idle all project services")
	}
	return nil
}

// Add idling IdledAtAnnotation to all services in a namespace
// This should only be called after calling AddProjectPreviousScaleAnnotation(), as it depends on the
// service's ScaleRefs annotation to determine which controllers belong to a service
func AddProjectIdledAtAnnotation(c *cache.Cache, namespace string, nowTime time.Time) error {
	failed := false
	svcs, err := c.GetProjectServices(namespace)
	if err != nil {
		return err
	}

	// Loop through all of the services in a namespace
	for _, obj := range svcs {
		svc := obj.(*cache.ResourceObject)
		err = AnnotateService(c, svc, nowTime, namespace, IdledAtAnnotation)
		if err != nil {
			glog.Errorf("Error idling service: %s", err)
			failed = true
		}
	}
	if failed {
		return errors.New("Failed to idle all project services")
	}
	return nil
}

// Adds the requested idling annotations to a service and all controllers in that service
func AnnotateService(c *cache.Cache, svc *cache.ResourceObject, nowTime time.Time, namespace, annotation string) error {
	endpointInterface := c.KubeClient.Endpoints(namespace)
	newEndpoint, err := c.GetAndCopyEndpoint(namespace, svc.Name)
	if err != nil {
		return errors.New(fmt.Sprintf("Error annotating Endpoint in namespace %s: %s\n", namespace, err))
	}

	if annotation == PreviousScaleAnnotation || annotation == FullIdleAnnotations {
		projectPods, err := c.GetProjectPods(namespace)
		if err != nil {
			return err
		}

		// Find the pods for this service, then find the scalable resources for those pods
		pods := c.GetPodsForService(svc, projectPods)
		resourceRefs, err := c.FindScalableResourcesForService(pods)
		if err != nil {
			return errors.New(fmt.Sprintf("Error scaling service: %s\n", err))
		}

		// Store the scalable resources in a map (this will become an annotation on the service later)
		scaleRefs := make(map[kapi.ObjectReference]*ControllerScaleReference)
		for ref := range resourceRefs {
			scaleRef, err := AnnotateController(c, ref, nowTime, annotation)
			if err != nil {
				return errors.New(fmt.Sprintf("Error annotating controller: %s\n", err))
			}
			scaleRefs[ref] = scaleRef
		}

		var endpointScaleRefs []*ControllerScaleReference
		for _, scaleRef := range scaleRefs {
			endpointScaleRefs = append(endpointScaleRefs, scaleRef)
		}
		scaleRefsBytes, err := json.Marshal(endpointScaleRefs)
		if err != nil {
			return errors.New(fmt.Sprintf("Error annotating Endpoint in namespace %s: %s\n", namespace, err))
		}

		// Add the scalable resources annotation to the service (endpoint)
		newEndpoint.Annotations[unidlingapi.UnidleTargetAnnotation] = string(scaleRefsBytes)
		// Need to delete any previous IdledAtAnnotations to prevent premature unidling
		if newEndpoint.Annotations[unidlingapi.IdledAtAnnotation] != "" {
			delete(newEndpoint.Annotations, unidlingapi.IdledAtAnnotation)
		}
	}

	var scaleRefs []ControllerScaleReference
	if annotation == IdledAtAnnotation || annotation == FullIdleAnnotations {
		// Add the annotation to the endpoint (service) and use the endpoints ScaleRef annotation to find
		// which controllers need to be annotated
		newEndpoint.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		scaleRefsBytes := newEndpoint.Annotations[unidlingapi.UnidleTargetAnnotation]
		err = json.Unmarshal([]byte(scaleRefsBytes), &scaleRefs)
		if err != nil {
			return errors.New(fmt.Sprintf("Error annotating DC in namespace %s when unmarshalling scaleRefsBytes from endpoint: %s\n", namespace, err))
		}
	}

	if annotation == IdledAtAnnotation {
		// Annotate the controllers
		// We only need to do this if we're specifically requesting the IdledAtAnnotation,
		// since FullIdleAnnotations would have already added IdledAt during the previous call to AnnotateController()
		for _, scaleRef := range scaleRefs {
			ref := kapi.ObjectReference{
				Name:      scaleRef.Name,
				Kind:      scaleRef.Kind,
				Namespace: svc.Namespace,
			}
			_, err := AnnotateController(c, ref, nowTime, annotation)
			if err != nil {
				return errors.New(fmt.Sprintf("Error annotating controller: %s\n", err))
			}
		}
	}

	_, err = endpointInterface.Update(newEndpoint)
	if err != nil {
		return errors.New(fmt.Sprintf("Error marshalling endpoint scale reference while annotating Endpoint in namespace %s: %s\n", namespace, err))
	}
	return nil
}

// Add idling annotations to a controller based on the `annotation` parameter
func AnnotateController(c *cache.Cache, ref kapi.ObjectReference, nowTime time.Time, annotation string) (*ControllerScaleReference, error) {
	obj, err := cache.GetController(ref, c.Factory)
	if err != nil {
		return nil, err
	}

	var replicas int32
	switch controller := obj.(type) {
	case *deployapi.DeploymentConfig:
		dcInterface := c.OsClient.DeploymentConfigs(controller.Namespace)
		copy, err := kapi.Scheme.DeepCopy(controller)
		if err != nil {
			return nil, err
		}
		newDC := copy.(*deployapi.DeploymentConfig)
		replicas = controller.Spec.Replicas
		if newDC.Annotations == nil {
			newDC.Annotations = make(map[string]string)
		}
		switch annotation {
		case FullIdleAnnotations:
			newDC.Annotations[unidlingapi.PreviousScaleAnnotation] = fmt.Sprintf("%v", controller.Spec.Replicas)
			newDC.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		case IdledAtAnnotation:
			newDC.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		case PreviousScaleAnnotation:
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
		rcInterface := c.KubeClient.ReplicationControllers(controller.Namespace)
		copy, err := kapi.Scheme.DeepCopy(controller)
		if err != nil {
			return nil, err
		}
		newRC := copy.(*kapi.ReplicationController)
		replicas = controller.Spec.Replicas
		if newRC.Annotations == nil {
			newRC.Annotations = make(map[string]string)
		}
		switch annotation {
		case FullIdleAnnotations:
			newRC.Annotations[unidlingapi.PreviousScaleAnnotation] = fmt.Sprintf("%v", controller.Spec.Replicas)
			newRC.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		case IdledAtAnnotation:
			newRC.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		case PreviousScaleAnnotation:
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
