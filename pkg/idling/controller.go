package idling

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/hawkular/hawkular-client-go/metrics"
	"github.com/openshift/online-hibernation/pkg/cache"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"

	buildapi "github.com/openshift/origin/pkg/build/api"
	unidlingapi "github.com/openshift/origin/pkg/unidling/api"
	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/util/wait"
)

// IdlerConfig is the configuration for the hawkular client and idler.
type IdlerConfig struct {
	Exclude            map[string]bool
	Url                string
	IdleSyncPeriod     time.Duration
	IdleQueryPeriod    time.Duration
	CA                 string
	Token              string
	HawkularInsecure   bool
	Threshold          int
	SyncWorkers        int
	IdleDryRun         bool
	InCluster          bool
	ProjectSleepPeriod time.Duration
}

type Idler struct {
	factory     *clientcmd.Factory
	config      *IdlerConfig
	resources   *cache.Cache
	stopChannel <-chan struct{}
}

func NewIdler(ic *IdlerConfig, f *clientcmd.Factory, c *cache.Cache) *Idler {
	ctrl := &Idler{
		config:    ic,
		factory:   f,
		resources: c,
	}
	return ctrl
}

// Main function for controller
func (idler *Idler) Run(stopChan <-chan struct{}) {
	idler.stopChannel = stopChan

	// Spawn a goroutine to run project sync
	go wait.Until(idler.sync, idler.config.IdleSyncPeriod, stopChan)
}

// Spawns goroutines to sync projects
func (idler *Idler) sync() {
	glog.V(1).Infof("Auto-idler: Running project sync")
	projects, err := idler.resources.Indexer.ByIndex("ofKind", cache.ProjectKind)
	if err != nil {
		glog.Errorf("Auto-idler: Error getting projects for sync: %s", err)
		return
	}
	namespaces := make(chan string, len(projects))
	for i := 1; i <= idler.config.SyncWorkers; i++ {
		go idler.startWorker(namespaces)
	}
	for _, namespace := range projects {
		namespaces <- namespace.(*cache.ResourceObject).Name
	}
	close(namespaces)
}

func (idler *Idler) startWorker(namespaces <-chan string) {
	for namespace := range namespaces {
		err := idler.syncProject(namespace)
		if err != nil {
			glog.Errorf("Auto-idler: %v", err)
		}
	}
}

// Sync projects and idle if necessary.
func (idler *Idler) syncProject(namespace string) error {
	if idler.config.Exclude[namespace] {
		return nil
	}
	project, err := idler.resources.GetProject(namespace)
	if err != nil {
		return fmt.Errorf("Auto-idler: Error getting project: %v", err)
	}

	if !project.IsAsleep {
		glog.V(2).Infof("Auto-idler: Syncing project: %s ", project.Name)
		projPods, err := idler.resources.GetProjectPods(namespace)
		if err != nil {
			return fmt.Errorf("Auto-idler: Error getting pods in ( %s ): %#v", namespace, err)
		}
		for _, pod := range projPods {
			// Skip the project sync if any pod in the project has not been running for a complete idling cycle, or if controller hasn't been running
			// for complete idling cycle.
			if time.Since(pod.(*cache.ResourceObject).RunningTimes[0].Start) < idler.config.IdleQueryPeriod {
				glog.V(2).Infof("Auto-idler: Ignoring project( %s ), either pod( %s )or controller hasn't been running for complete idling cycle.", namespace, pod.(*cache.ResourceObject).Name)
				glog.V(2).Infof("Auto-idler: Project( %s )sync complete.", namespace)
				return nil
			}
		}

		svcs, err := idler.resources.GetProjectServices(namespace)
		if err != nil {
			return fmt.Errorf("Auto-idler: Error getting services in ( %s ): %#v", namespace, err)
		}
		var scalablePods []interface{}
		var runningPods []interface{}
		for _, svc := range svcs {
			// Only consider pods with services for idling.
			scalablePods := idler.resources.GetPodsForService(svc.(*cache.ResourceObject), projPods)
			for _, pod := range scalablePods {
				runningPods = append(runningPods, pod)
			}
		}

		// If true, endpoints in namespace are currently idled,
		// i.e. endpoints in project have idling annotations and len(scalablePods)==0
		// Project is not currently idled if there are any running scalable pods in namespace.
		idled, err := idler.checkIdledState(namespace, len(runningPods))
		if err != nil {
			return err
		}
		if !idled {
			if err := idler.idleIfInactive(namespace, scalablePods); err != nil {
				glog.Errorf("Auto-idler: Error syncing project( %s ): %s", namespace, err)
			}
		} else {
			glog.V(2).Infof("Auto-idler: Project( %s )sync complete.", namespace)
			return nil
		}
	} else {
		glog.V(2).Infof("Auto-idler: Ignoring project( %s ), currently in force-sleep.", namespace)
		glog.V(2).Infof("Auto-idler: Project( %s )sync complete.", namespace)
		return nil
	}
	glog.V(2).Infof("Auto-idler: Project( %s )sync complete.", namespace)
	return nil
}

func (idler *Idler) idleIfInactive(namespace string, pods []interface{}) error {
	for _, obj := range pods {
		pod := obj.(*cache.ResourceObject)
		if _, ok := pod.Labels[buildapi.BuildAnnotation]; ok {
			if pod.IsStarted() {
				glog.V(2).Infof("Auto-idler: Ignoring project( %s ), builder pod( %s ) found in project.", namespace, pod.Name)
				return nil
			}
		}
		if time.Since(pod.RunningTimes[0].Start) < idler.config.IdleQueryPeriod {
			glog.V(2).Infof("Auto-idler: Ignoring project( %s ), either pod( %s )or controller hasn't been running for complete idling cycle.", namespace, pod.Name)
			return nil
		}
	}
	tags := make(map[string]string)
	var filters []metrics.Modifier
	project, err := idler.resources.GetProject(namespace)
	if err != nil {
		glog.Errorf("Auto-idler: Error getting project: %v", err)
	}
	if !project.IsAsleep {
		tags["descriptor_name"] = "network/rx"
		tags["type"] = "pod"

		p := idler.getMetricsParams(namespace)

		period := -1 * idler.config.IdleQueryPeriod
		endtime := time.Now()
		starttime := endtime.Add(period)
		threshold := idler.config.Threshold
		filters = append(filters, metrics.Filters(metrics.TagsFilter(tags), metrics.BucketsFilter(1), metrics.StartTimeFilter(starttime), metrics.EndTimeFilter(endtime), metrics.StackedFilter()))

		c, err := metrics.NewHawkularClient(p)
		if err != nil {
			return err
		}

		bp, err := c.ReadBuckets(metrics.Counter, filters...)
		if err != nil {
			return err
		}

		for _, value := range bp {
			activity := value.Max - value.Min
			glog.V(2).Infof("PROJECT( %s )NETWORK_ACTIVITY(bytes): %v", namespace, int(activity))

			if int(activity) < threshold {
				glog.V(3).Infof("Project( %s )MaxNetworkRX: %#v", namespace, value.Max)
				glog.V(3).Infof("Project( %s )MinNetworkRX: %#v", namespace, value.Min)
				if !idler.config.IdleDryRun {
					glog.V(2).Infof("Project( %s )activity below idling threshold, idling....", namespace)
					err := IdleProjectServices(idler.resources, namespace)
					if err != nil {
						glog.Errorf("Auto-idler: Namespace( %s ): %s", namespace, err)
					}
				} else {
					glog.V(2).Infof("Project( %s )network activity below idling threshold, but currently in DryRun mode.", namespace)
				}
			} else {
				glog.V(3).Infof("Project( %s )MaxNetworkRX: %#v", namespace, value.Max)
				glog.V(3).Infof("Project( %s )MinNetworkRX: %#v", namespace, value.Min)
				glog.V(2).Infof("Project( %s )network activity is above idling threshold.", namespace)
			}
		}
	} else {
		glog.V(2).Infof("Auto-idler: Ignoring project( %s ), currently in force-sleep.", namespace)
	}

	return nil
}

// getMetricsParams initializes Hawkular Parameters.
func (idler *Idler) getMetricsParams(namespace string) metrics.Parameters {
	p := metrics.Parameters{
		Tenant: namespace,
		Url:    idler.config.Url,
		Token:  idler.config.Token,
	}

	// Authentication parameters
	tC := &tls.Config{}
	if idler.config.InCluster && !idler.config.HawkularInsecure {
		caCert := []byte(idler.config.CA)

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tC.RootCAs = caCertPool
	} else {
		if idler.config.CA == "" {
			tC.InsecureSkipVerify = true
		}
	}

	if idler.config.HawkularInsecure {
		tC.InsecureSkipVerify = true
	}

	p.TLSConfig = tC

	glog.V(2).Infof("Auto-idler: Initialised Hawkular for project( %s )", namespace)
	return p
}

// checkIdledState returns true if project is idled, false if project is not currently idled
func (idler *Idler) checkIdledState(namespace string, numScalablePods int) (bool, error) {
	endpointInterface := idler.resources.KubeClient.Endpoints(namespace)
	epList, err := endpointInterface.List(kapi.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("Auto-idler: Project( %s ): %s", namespace, err)
	}
	for _, ep := range epList.Items {
		if _, ok := ep.ObjectMeta.Annotations[unidlingapi.IdledAtAnnotation]; ok && numScalablePods == 0 {
			glog.V(2).Infof("Auto-idler: Project( %s )already in idled state.", namespace)
			return true, nil
		}
	}
	glog.V(3).Infof("Auto-idler: Project( %s )not currently idled, checking network activity received...", namespace)
	return false, nil
}
