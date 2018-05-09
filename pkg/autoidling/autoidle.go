package autoidling

import (
	"fmt"
	"time"

	"github.com/golang/glog"

	"github.com/openshift/online-hibernation/pkg/cache"

	"golang.org/x/net/context"

	"github.com/prometheus/client_golang/api/prometheus"
	"github.com/prometheus/common/model"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const MaxRetries = 2

// AutoIdlerConfig is the configuration for the prometheus client and idler.
type AutoIdlerConfig struct {
	PrometheusClient   prometheus.Client
	IdleSyncPeriod     time.Duration
	IdleQueryPeriod    time.Duration
	Threshold          int
	SyncWorkers        int
	IdleDryRun         bool
	ProjectSleepPeriod time.Duration
}

// AutoIdler is the auto-idler object
type AutoIdler struct {
	config        *AutoIdlerConfig
	resourceStore *cache.ResourceStore
	queue         workqueue.RateLimitingInterface
	stopChan      <-chan struct{}
}

// NewAutoIdler returns an AutoIdler object
func NewAutoIdler(ic *AutoIdlerConfig, rs *cache.ResourceStore) *AutoIdler {
	ctrl := &AutoIdler{
		config:        ic,
		resourceStore: rs,
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "auto-idler"),
	}
	return ctrl
}

// Run starts the idler controller and keeps it going until
// the given channel is closed.
func (idler *AutoIdler) Run(stopChan <-chan struct{}) {
	idler.stopChan = stopChan
	go wait.Until(idler.getNetmapAndSync, idler.config.IdleSyncPeriod, stopChan)
	for i := 1; i <= idler.config.SyncWorkers; i++ {
		go wait.Until(idler.startWorker(), time.Second, stopChan)
	}
	<-stopChan
	glog.V(2).Infof("Auto-idler: Shutting down controller")
	idler.queue.ShutDown()
}

// GetNetworkActivity queries prometheus to get a sum of network activity received
// over the IdleQueryPeriod for all pods, separated by namespace.  Only results with
// network activity below the idler threshold are returned.  This function returns
// a map of namespace:networkActivity (bytes).
func (idler *AutoIdler) GetNetworkActivity() map[string]float64 {
	glog.V(2).Infof("Auto-idler: Querying prometheus to get  network activity for projects")
	queryAPI := prometheus.NewQueryAPI(idler.config.PrometheusClient)
	// idler.conifg.Threshold can only be set via command line flags
	query_string := fmt.Sprintf(`sum(delta(container_network_receive_bytes_total{namespace!=""}[%s])) by (namespace) < %v`, model.Duration(idler.config.IdleQueryPeriod).String(), idler.config.Threshold)
	result, err := queryAPI.Query(context.TODO(), query_string, time.Now())
	if err != nil {
		glog.Errorf("Auto-idler: %s", err)
		return nil
	}
	var namespace string
	netmap := make(map[string]float64)
	vec := result.(model.Vector)
	for _, v := range vec {
		nsMap := v.Metric
		val := v.Value
		for _, ns := range nsMap {
			namespace = string(ns)
		}
		netmap[namespace] = float64(val)
	}
	glog.V(2).Infof("Auto-idler: Prometheus query complete")
	return netmap
}

// sync matches projects that should be idled and adds them to the workqueue.
// If a project has scalable resources and is in the map of projects below the idling
// threshold and if the idler is not in DryRun mode, then that project name is
// added to the queue of projects to be idled.
func (idler *AutoIdler) sync(netmap map[string]float64) {
	glog.V(1).Infof("Auto-idler: Running project sync")
	projects, err := idler.resourceStore.ProjectList.List(labels.Everything())
	if err != nil {
		glog.Errorf("Auto-idler: %s", err)
		return
	}
	for _, p := range projects {
		// TODO: Not sure why "" is returned here, but it is...
		if p.Name == "" {
			continue
		}
		scalable, err := idler.checkForScalables(p.Name)
		if err != nil {
			glog.Errorf("Auto-idler: %s", err)
			continue
		}
		if !scalable {
			glog.V(2).Infof("Auto-idler: Project( %s )sync complete", p.Name)
			continue
		}
		projInMap := false
		if bytes, ok := netmap[p.Name]; ok {
			projInMap = true
			if idler.config.IdleDryRun {
				glog.V(2).Infof("Auto-idler: Project( %s )netrx bytes( %v )below idling threshold, but currently in DryRun mode", p.Name, bytes)
			} else {
				glog.V(2).Infof("Auto-idler: Project( %s )netrx bytes( %v )below idling threshold", p.Name, bytes)
				idler.queue.Add(p.Name)
			}
		}
		if !projInMap {
			glog.V(2).Infof("Auto-idler: Project( %s )sync complete", p.Name)
		}
	}
}

// getNetmapAndSync allows for unit testing, to test sync code
// with a fake prometheus query return
func (idler *AutoIdler) getNetmapAndSync() {
	netmap := idler.GetNetworkActivity()
	if len(netmap) == 0 {
		glog.V(2).Infof("Auto-idler: Unable to sync projects, no data obtained from prometheus")
		return
	}
	idler.sync(netmap)
}

// startWorker gets items from the queue hands them to syncProject
func (idler *AutoIdler) startWorker() func() {
	workFunc := func() bool {
		ns, quit := idler.queue.Get()
		if quit {
			return true
		}
		defer idler.queue.Done(ns)
		if err := idler.syncProject(ns.(string)); err != nil {
			if idler.queue.NumRequeues(ns) > MaxRetries {
				glog.V(2).Infof("Auto-idler: Unable to sync( %s ), no more retries left", ns.(string))
				idler.queue.Forget(ns)
				return false
			} else {
				glog.V(2).Infof("Auto-idler: Unable to fully sync( %s ), requeuing: %v", ns.(string), err)
				idler.queue.AddRateLimited(ns)
				return false
			}
		} else {
			idler.queue.Forget(ns)
			return false
		}
	}
	return func() {
		for {
			if quit := workFunc(); quit {
				glog.V(2).Infof("Auto-idler: Worker shutting down")
				return
			}
		}
	}
}

// syncProject gets project from the cache, does a final check for
// IsAsleep and IsIdled and then calls autoIdleProjectServices.
func (idler *AutoIdler) syncProject(namespace string) error {
	isAsleep, err := idler.resourceStore.IsAsleep(namespace)
	if err != nil {
		return err
	}
	if isAsleep {
		glog.V(2).Infof("Auto-idler: Ignoring project( %s ), currently in force-sleep", namespace)
		return nil
	}
	glog.V(2).Infof("Auto-idler: Syncing project: %s ", namespace)
	err = idler.autoIdleProjectServices(namespace)
	if err != nil {
		return err
	}
	glog.V(2).Infof("Auto-idler: Project( %s )sync complete", namespace)
	return nil
}

// checkForScalables returns true if there are scalable resources
// associated with pods in a namespace
func (idler *AutoIdler) checkForScalables(namespace string) (bool, error) {
	scalable := false
	// Extra check here, not sure it's necessary
	isAsleep, err := idler.resourceStore.IsAsleep(namespace)
	if err != nil {
		return scalable, err
	}
	if isAsleep {
		glog.V(2).Infof("Auto-idler: Ignoring project( %s ), currently in force-sleep.", namespace)
		return scalable, nil
	}
	svcs, err := idler.resourceStore.ServiceList.Services(namespace).List(labels.Everything())
	if err != nil {
		return scalable, err
	}
	if len(svcs) == 0 {
		glog.V(2).Infof("Auto-idler: Project( %s )has no services", namespace)
		return scalable, nil
	}

	projectPods, err := idler.resourceStore.PodList.Pods(namespace).List(labels.Everything())
	if err != nil {
		return scalable, err
	}

	if len(projectPods) == 0 {
		glog.V(2).Infof("Auto-idler: No pods found in project( %s )", namespace)
		return scalable, nil
	}

	for _, pod := range projectPods {
		if _, ok := pod.ObjectMeta.Labels[cache.BuildAnnotation]; ok {
			if pod.Status.Phase == corev1.PodRunning {
				glog.V(2).Infof("Auto-idler: Ignoring project( %s ), builder pod( %s ) found in project.", pod.Namespace, pod.Name)
				return scalable, nil
			}
		}
		if time.Since(pod.Status.StartTime.Time) < idler.config.IdleQueryPeriod {
			glog.V(2).Infof("Auto-idler: Ignoring project( %s ), pod( %s )hasn't been running for complete idling cycle.", pod.Namespace, pod.Name)
			return scalable, nil
		}
	}
	var scalablePods []*corev1.Pod
	for _, svc := range svcs {
		// Only consider pods with services for idling.
		podsWService := idler.resourceStore.GetPodsForService(svc, projectPods)
		for _, pod := range podsWService {
			scalablePods = append(scalablePods, pod)
		}
	}
	// if there are no running pods, project can't be idled, or might already be idled
	if len(scalablePods) == 0 {
		glog.V(2).Infof("Auto-idler: No pods associated with services in project( %s )", namespace)
		return scalable, nil
	}

	scalable = true
	return scalable, nil
}

// autoIdleProjectServices re-creates `oc idle`
// `oc idle` links controllers to endpoints by:
//   1. taking in an endpoint (service name)
//   2. getting the pods on that endpoint
//   3. getting the controller on each pod (and, in the case of DCs, getting the controller on that controller)
// This approach is:
//   1. loop through all services in project
//   2. for each service, get pods on service by label selector
//   3. get the controller on each pod (and, in the case of DCs, get the controller on that controller)
//   4. get endpoint with the same name as the service
func (idler *AutoIdler) autoIdleProjectServices(namespace string) error {
	nowTime := time.Now()
	isAsleep, err := idler.resourceStore.IsAsleep(namespace)
	if err != nil {
		return err
	}
	if isAsleep {
		glog.V(2).Infof("Auto-idler: Project( %s )currently in force-sleep, exiting...", namespace)
		return nil
	}

	glog.V(0).Infof("Auto-idler: Adding previous scale annotation in project( %s )", namespace)
	err = idler.resourceStore.AddProjectPreviousScaleAnnotation(namespace)
	if err != nil {
		return fmt.Errorf("Error adding project( %s )previous scale annotation: %s", namespace, err)
	}

	glog.V(2).Infof("Auto-idler: Scaling  objects in project %s", namespace)
	err = idler.resourceStore.ScaleAllScalableObjectsInNamespace(namespace)
	if err != nil {
		return fmt.Errorf("Error scaling objects in project %s: %s", namespace, err)
	}

	glog.V(0).Infof("Auto-idler: Adding idled-at annotation in project( %s )", namespace)
	err = idler.resourceStore.AddProjectIdledAtAnnotation(namespace, nowTime, idler.config.ProjectSleepPeriod)
	if err != nil {
		return fmt.Errorf("Error adding idled-at annotation in project( %s ): %v", namespace, err)
	}

	glog.V(2).Infof("Auto-idler: Project( %s )endpoints are now idled", namespace)
	return nil
}
