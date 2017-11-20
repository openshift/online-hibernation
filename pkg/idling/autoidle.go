package idling

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/openshift/online-hibernation/pkg/cache"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"
	"github.com/prometheus/client_golang/api/prometheus"
	"github.com/prometheus/common/model"

	buildapi "github.com/openshift/origin/pkg/build/api"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/util/workqueue"
)

const MaxRetries = 2

// IdlerConfig is the configuration for the prometheus client and idler.
type IdlerConfig struct {
	Exclude            map[string]bool
	PrometheusClient   prometheus.Client
	IdleSyncPeriod     time.Duration
	IdleQueryPeriod    time.Duration
	Threshold          int
	SyncWorkers        int
	IdleDryRun         bool
	ProjectSleepPeriod time.Duration
}

// Idler is the auto-idler object
type Idler struct {
	factory           *clientcmd.Factory
	config            *IdlerConfig
	resources         *cache.Cache
	queue             workqueue.RateLimitingInterface
	stopChannel       <-chan struct{}
}

// NewIdler returns an Idler object
//func NewIdler(ic *IdlerConfig, f *clientcmd.Factory, c *cache.Cache, pmi PrometheusMetricsInterface, pm PrometheusMetrics) *Idler {
func NewIdler(ic *IdlerConfig, f *clientcmd.Factory, c *cache.Cache) *Idler {
	ctrl := &Idler{
		config:            ic,
		factory:           f,
		resources:         c,
		queue:             workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "auto-idler"),
	}
	return ctrl
}

// Run starts the idler controller and keeps it going until
// the given channel is closed.
func (idler *Idler) Run(stopChan <-chan struct{}) {
	idler.stopChannel = stopChan
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
func (pm *Idler) GetNetworkActivity() map[string]float64 {
	glog.V(2).Infof("Auto-idler: Querying prometheus to get  network activity for projects")
	queryAPI := prometheus.NewQueryAPI(pm.config.PrometheusClient)
	// pm.conifg.Threshold can only be set via command line flags
	query_string := fmt.Sprintf(`sum(delta(container_network_receive_bytes_total{namespace!=""}[%s])) by (namespace) < %v`, model.Duration(pm.config.IdleQueryPeriod).String(), pm.config.Threshold)
	result, err := queryAPI.Query(context.TODO(), query_string, time.Now())
	if err != nil {
		glog.Errorf("Auto-idler: %s", err)
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
// If a project has scalable resources, is not in
// the exclude_namespace list, is in the map of projects below the idling
// threshold and if the idler is not in DryRun mode, then that project name is
// added to the queue of projects to be idled.
func (idler *Idler) sync(netmap map[string]float64) {
	glog.V(1).Infof("Auto-idler: Running project sync")
	projects, err := idler.resources.Indexer.ByIndex("ofKind", cache.ProjectKind)
	if err != nil {
		glog.Errorf("Auto-idler: %s", err)
		return
	}
	for _, namespace := range projects {
		ns := namespace.(*cache.ResourceObject).Name
		scalable, err := idler.checkForScalables(ns)
		if err != nil {
			glog.Errorf("Auto-idler: %s", err)
			continue
		}
		if !scalable {
			glog.V(2).Infof("Auto-idler: Project( %s )sync complete", ns)
			continue
		}
		if !idler.config.Exclude[ns] {
			nsInMap := false
			if bytes, ok := netmap[ns]; ok {
				nsInMap = true
				if idler.config.IdleDryRun {
					glog.V(2).Infof("Auto-idler: Project( %s )netrx bytes( %v )below idling threshold, but currently in DryRun mode", ns, bytes)
				} else {
					glog.V(2).Infof("Auto-idler: Project( %s )netrx bytes( %v )below idling threshold", ns, bytes)
					idler.queue.Add(ns)
				}
			}
			if !nsInMap {
				// Only way to get here is if proj has scalable pods but not in returned map of below_threshold projs
				glog.V(2).Infof("Auto-idler: Project( %s )network activity above idling threshold", ns)
				glog.V(2).Infof("Auto-idler: Project( %s )sync complete", ns)
			}
		}
	}
}

// getNetmapAndSync allows for unit testing, to test sync code
// with a fake prometheus query return
func (idler *Idler) getNetmapAndSync() {
	netmap := idler.GetNetworkActivity()
	idler.sync(netmap)
}

// startWorker gets items from the queue hands them to syncProject
func (idler *Idler) startWorker() func() {
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
func (idler *Idler) syncProject(namespace string) error {
	project, err := idler.resources.GetProject(namespace)
	if err != nil {
		return err
	}
	if project.IsAsleep {
		glog.V(2).Infof("Auto-idler: Ignoring project( %s ), currently in force-sleep", namespace)
		return nil
	}
	glog.V(2).Infof("Auto-idler: Syncing project: %s ", project.Name)
	glog.V(2).Infof("Auto-idler: Idling project( %s )", namespace)
	err = idler.autoIdleProjectServices(namespace)
	if err != nil {
		return err
	}
	glog.V(2).Infof("Auto-idler: Project( %s )sync complete", namespace)
	return nil
}

// checkForScalables returns true if there are scalable resources
// associated with pods in a namespace
func (idler *Idler) checkForScalables(namespace string) (bool, error) {
	scalable := false
	project, err := idler.resources.GetProject(namespace)
	if err != nil {
		return scalable, err
	}
	// Extra check here, not sure it's necessary
	if project.IsAsleep {
		glog.V(2).Infof("Auto-idler: Ignoring project( %s ), currently in force-sleep.", namespace)
		return scalable, nil
	}
	svcs, err := idler.resources.GetProjectServices(namespace)
	if err != nil {
		return scalable, err
	}
	if len(svcs) == 0 {
		glog.V(2).Infof("Auto-idler: Project( %s )has no services", namespace)
		return scalable, nil
	}

	projectPods, err := idler.resources.GetProjectPods(namespace)
	if err != nil {
		return scalable, err
	}

	for _, podObj := range projectPods {
		pod := podObj.(*cache.ResourceObject)
		if _, ok := pod.Labels[buildapi.BuildAnnotation]; ok {
			if pod.IsStarted() {
				glog.V(2).Infof("Auto-idler: Ignoring project( %s ), builder pod( %s ) found in project.", namespace, pod.Name)
				return scalable, nil
			}
		}
		if time.Since(pod.RunningTimes[0].Start) < idler.config.IdleQueryPeriod {
			glog.V(2).Infof("Auto-idler: Ignoring project( %s ), either pod( %s )or controller hasn't been running for complete idling cycle.", namespace, pod.Name)
			return scalable, nil
		}
	}
	var scalablePods []interface{}
	for _, svc := range svcs {
		// Only consider pods with services for idling.
		podsWService := idler.resources.GetPodsForService(svc.(*cache.ResourceObject), projectPods)
		for _, pod := range podsWService {
			scalablePods = append(scalablePods, pod)
		}
	}
	// if there are no running pods, project can't be idled
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
func (idler *Idler) autoIdleProjectServices(namespace string) error {
	nowTime := time.Now()
	project, err := idler.resources.GetProject(namespace)
	if err != nil {
		return err
	}
	if project.IsAsleep {
		glog.V(2).Infof("Auto-idler: Project( %s )currently in force-sleep, exiting...", namespace)
		return nil
	}
	glog.V(0).Infof("Auto-idler: Adding previous scale annotation in project( %s )", namespace)
	err = AddProjectPreviousScaleAnnotation(idler.resources, namespace)
	if err != nil {
		return fmt.Errorf("Error adding project( %s )previous scale annotation: %s", namespace, err)
	}
	glog.V(2).Infof("Auto-idler: Scaling DCs in project %s", namespace)
	err = ScaleProjectDCs(idler.resources, namespace)
	if err != nil {
		return fmt.Errorf("Error scaling DCs in project %s: %s", namespace, err)
	}

	glog.V(2).Infof("Auto-idler: Scaling RCs in project %s", namespace)
	err = ScaleProjectRCs(idler.resources, namespace)
	if err != nil {
		return fmt.Errorf("Error scaling RCs in project %s: %s", namespace, err)
	}
	glog.V(2).Infof("Auto-idler: Deleting pods in project %s", namespace)
	err = DeleteProjectPods(idler.resources, namespace)
	if err != nil {
		return fmt.Errorf("Error deleting pods in project %s: %s", namespace, err)
	}
	glog.V(0).Infof("Auto-idler: Adding idled-at annotation in project( %s )", namespace)
	err = AddProjectIdledAtAnnotation(idler.resources, namespace, nowTime, idler.config.ProjectSleepPeriod)
	if err != nil {
		return fmt.Errorf("Error adding idled-at annotation in project( %s ): %v", namespace, err)
	}
	glog.V(2).Infof("Auto-idler: Project( %s )endpoints are now idled", namespace)
	return nil
}
