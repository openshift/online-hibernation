package idling

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"

	deployapi "github.com/openshift/origin/pkg/deploy/api"
	unidlingapi "github.com/openshift/origin/pkg/unidling/api"

	"github.com/golang/glog"

	kapi "k8s.io/kubernetes/pkg/api"
	kerrors "k8s.io/kubernetes/pkg/api/errors"
)

// The unidlingapi constants are copied over for consistency
const (
	IdledAtAnnotation       = unidlingapi.IdledAtAnnotation
	PreviousScaleAnnotation = unidlingapi.PreviousScaleAnnotation
	UnidleTargetAnnotation  = unidlingapi.UnidleTargetAnnotation
)

type ControllerScaleReference struct {
	Kind     string
	Name     string
	Replicas int32
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
			if !kerrors.IsNotFound(err) {
				glog.Errorf("Project( %s) DC( %s ): %s", namespace, dc.Name, err)
				failed = true
			}
		}
	}
	if failed {
		return fmt.Errorf("Failed to scale all project( %s)DCs", namespace)
	}
	return nil
}

func ScaleProjectRCs(c *cache.Cache, namespace string) error {
	// Scale RCs to 0
	rcInterface := c.KubeClient.ReplicationControllers(namespace)
	rcList, err := rcInterface.List(kapi.ListOptions{})
	if err != nil {
		return err
	}

	failed := false
	for _, thisRC := range rcList.Items {
		// TODO: Use indexer function 'rcByDC' here for efficiency?
		// Given thisRC name, check to see if thisRC has a DC
		// If thisRC does not have an associated DC, then scale the RC,
		if _, exists := thisRC.Spec.Selector["deploymentconfig"]; !exists {
			copy, err := kapi.Scheme.DeepCopy(thisRC)
			if err != nil {
				return err
			}
			newRC := copy.(kapi.ReplicationController)
			newRC.Spec.Replicas = 0
			_, err = rcInterface.Update(&newRC)
			if err != nil {
				if !kerrors.IsNotFound(err) {
					glog.Errorf("Project( %s) RC( %s ): %s", namespace, thisRC.Name, err)
					failed = true
				}
			}
		} else {
			dc := thisRC.Spec.Selector["deploymentconfig"]
			glog.V(3).Infof("Skipping RC( %s ), already scaled associated DC( %s )", thisRC.Name, dc)
		}
	}
	if failed {
		return fmt.Errorf("Failed to scale all project( %s )RCs", namespace)
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
			if !kerrors.IsNotFound(err) {
				glog.Errorf("Project( %s ) Pod( %s ): %s", namespace, pod.Name, err)
				failed = true
			}
		}
	}
	if failed {
		return fmt.Errorf("Failed to delete all project( %s )pods", namespace)
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
		glog.V(2).Infof("Adding previous scale annotation to service( %s )", svc.Name)
		err = AnnotateService(c, svc, time.Time{}, namespace, PreviousScaleAnnotation)
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
func AddProjectIdledAtAnnotation(c *cache.Cache, namespace string, nowTime time.Time, sleepPeriod time.Duration) error {
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
			glog.Errorf("Project( %s ) Service( %s ): %s", namespace, svc.Name, err)
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("Failed to add idled-at annotation to all project( %s )services", namespace)
	}
	return nil
}

// Adds the requested idling annotations to a service and all controllers in that service
func AnnotateService(c *cache.Cache, svc *cache.ResourceObject, nowTime time.Time, namespace, annotation string) error {
	project, err := c.GetProject(namespace)
	if err != nil {
		return err
	}
	// TODO: Limitation of oc idle and auto-idler: Cannot handle 2 services sharing a single RC.
	// Projects with 2 services sharing an RC will have unpredictable behavior of idling/auto-idling
	// This needs to be fixed in oc idle code.  Will document with auto-idling documentation for now.
	if annotation == PreviousScaleAnnotation {
		endpointInterface := c.KubeClient.Endpoints(namespace)
		newEndpoint, err := c.GetAndCopyEndpoint(namespace, svc.Name)
		if err != nil {
			return err
		}
		// Need to delete any previous IdledAtAnnotations to prevent premature unidling
		if newEndpoint.Annotations[unidlingapi.IdledAtAnnotation] != "" {
			if project.IsAsleep {
				glog.V(2).Infof("Force-sleeper: Removing stale idled-at annotation in endpoint( %s ) project( %s )", svc.Name, namespace)
			} else {
				glog.V(2).Infof("Auto-idler: Removing stale idled-at annotation in endpoint( %s ) project( %s )", svc.Name, namespace)
			}
			delete(newEndpoint.Annotations, unidlingapi.IdledAtAnnotation)
		}
		projectPods, err := c.GetProjectPods(namespace)
		if err != nil {
			return err
		}
		// Find the pods for this service, then find the scalable resources for those pods
		pods := c.GetPodsForService(svc, projectPods)
		resourceRefs, err := c.FindScalableResourcesForService(pods)
		if err != nil {
			return err
		}
		// Store the scalable resources in a map (this will become an annotation on the service later)
		scaleRefs := make(map[kapi.ObjectReference]*ControllerScaleReference)
		for ref := range resourceRefs {
			scaleRef, err := AnnotateController(c, ref, nowTime, annotation, project.IsAsleep)
			if err != nil {
				return err
			}
			scaleRefs[ref] = scaleRef
		}

		var endpointScaleRefs []*ControllerScaleReference
		for _, scaleRef := range scaleRefs {
			endpointScaleRefs = append(endpointScaleRefs, scaleRef)
		}
		scaleRefsBytes, err := json.Marshal(endpointScaleRefs)
		if err != nil {
			return err
		}

		epList, err := endpointInterface.List(kapi.ListOptions{})
		if err != nil {
			return err
		}
		for _, ep := range epList.Items {
			_, targetExists := ep.ObjectMeta.Annotations[unidlingapi.UnidleTargetAnnotation]
			// TODO: It's possible that the unidle target that already exists has a replica that is
			// not up-to-date.  For instance, a project was *manually* unidled, then scaled.  Then if
			// the project is auto-idled, the unidle target annotation will hold the replicas of the
			// original scale, before the project was manually scaled.
			// For now, skip setting new unidle target when one already exists in an endpoint,
			// at the risk of keeping an outdated scale, to prevent unidle-target being set to 0. It's
			// not currently possible to predict if project is currently in the process of becoming idled.
			// This needs to be fixed in oc idle/web-console code.
			if targetExists {
				if !project.IsAsleep {
					glog.V(2).Infof("Auto-idler: Endpoint( %s )has unidle target annotation, skipping previous scale annotation", ep.Name)
				} else {
					glog.V(2).Infof("Force-sleeper: Endpoint( %s )has unidle target annotation, skipping previous scale annotation", ep.Name)
				}
			} else {
				// Add the scalable resources annotation to the service (endpoint)
				newEndpoint.Annotations[unidlingapi.UnidleTargetAnnotation] = string(scaleRefsBytes)
			}
		}
		_, err = endpointInterface.Update(newEndpoint)
		if err != nil {
			return err
		}
		return nil

	}
	var scaleRefs []ControllerScaleReference
	if annotation == IdledAtAnnotation {
		endpointInterface := c.KubeClient.Endpoints(namespace)
		newEndpoint, err := c.GetAndCopyEndpoint(namespace, svc.Name)
		if err != nil {
			return err
		}
		// Add the annotation to the endpoint (service) and use the endpoints ScaleRef annotation to find
		// which controllers need to be annotated
		newEndpoint.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		scaleRefsBytes := newEndpoint.Annotations[unidlingapi.UnidleTargetAnnotation]
		err = json.Unmarshal([]byte(scaleRefsBytes), &scaleRefs)
		if err != nil {
			return err
		}
		_, err = endpointInterface.Update(newEndpoint)
		if err != nil {
			return err
		}

		// Annotate the controllers
		for _, scaleRef := range scaleRefs {
			ref := kapi.ObjectReference{
				Name:      scaleRef.Name,
				Kind:      scaleRef.Kind,
				Namespace: svc.Namespace,
			}
			_, err := AnnotateController(c, ref, nowTime, annotation, project.IsAsleep)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Add idling annotations to a controller based on the `annotation` parameter
func AnnotateController(c *cache.Cache, ref kapi.ObjectReference, nowTime time.Time, annotation string, isAsleep bool) (*ControllerScaleReference, error) {
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
		case IdledAtAnnotation:
			newDC.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		case PreviousScaleAnnotation:
			if newDC.Annotations[unidlingapi.IdledAtAnnotation] != "" {
				if isAsleep {
					glog.V(2).Infof("Force-sleeper: Removing stale idled-at annotation in DC( %s ) project( %s )", newDC.Name, controller.Namespace)
				} else {
					glog.V(2).Infof("Auto-idler: Removing stale idled-at annotation in DC( %s ) project( %s )", newDC.Name, controller.Namespace)
				}
				delete(newDC.Annotations, unidlingapi.IdledAtAnnotation)
			}
			newDC.Annotations[unidlingapi.PreviousScaleAnnotation] = fmt.Sprintf("%v", controller.Spec.Replicas)
		}
		_, err = dcInterface.Update(newDC)
		if err != nil {
			if kerrors.IsNotFound(err) {
				return nil, nil
			} else {
				return nil, err
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
		case IdledAtAnnotation:
			newRC.Annotations[unidlingapi.IdledAtAnnotation] = nowTime.Format(time.RFC3339)
		case PreviousScaleAnnotation:
			if newRC.Annotations[unidlingapi.IdledAtAnnotation] != "" {
				if isAsleep {
					glog.V(2).Infof("Force-sleeper: Removing stale idled-at annotation in RC( %s ) project( %s )", newRC.Name, controller.Namespace)
				} else {
					glog.V(2).Infof("Auto-idler: Removing stale idled-at annotation in RC( %s ) project( %s )", newRC.Name, controller.Namespace)
				}
				delete(newRC.Annotations, unidlingapi.IdledAtAnnotation)
			}
			newRC.Annotations[unidlingapi.PreviousScaleAnnotation] = fmt.Sprintf("%v", controller.Spec.Replicas)
		}

		_, err = rcInterface.Update(newRC)
		if err != nil {
			if kerrors.IsNotFound(err) {
				return nil, nil
			} else {
				return nil, err
			}
		}
	}
	return &ControllerScaleReference{
		Kind:     ref.Kind,
		Name:     ref.Name,
		Replicas: replicas}, nil
}
