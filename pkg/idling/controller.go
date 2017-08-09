package idling

import (
	"crypto/tls"
	"crypto/x509"
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
	glog.V(1).Infof("Auto-idler: Running project sync\n")
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
		idler.syncProject(namespace)
	}
}

// Sync projects and idle if necessary.
func (idler *Idler) syncProject(namespace string) {
	if idler.config.Exclude[namespace] {
		return
	}
	project, err := idler.resources.GetProject(namespace)
	if err != nil {
		glog.Errorf("Auto-idler: Error getting project: %v\n", err)
	}

	if !project.IsAsleep {
		glog.V(2).Infof("Auto-idler: Syncing project: %s \n", project.Name)
		var runningpods []interface{}
		pods, err := idler.resources.Indexer.ByIndex("ofKind", cache.PodKind)
		if err != nil {
			glog.Errorf("Auto-idler: Error getting pods: %#v", err)
		}
		for _, pod := range pods {
			if pod.(*cache.ResourceObject).Namespace == namespace && pod.(*cache.ResourceObject).IsStarted() {
				runningpods = append(runningpods, pod)
			}
		}
		//If false, project is not currently idled
		if !idler.checkIdledState(namespace) {
			if len(runningpods) != 0 {
				if err := idler.idleIfInactive(namespace, runningpods); err != nil {
					glog.Errorf("Auto-idler: Error syncing project( %s ): %s", namespace, err)
				}
			} else {
				glog.V(2).Infof("Auto-idler: Skipping project( %s ), no running pods found", namespace)
			}
		} else {
			glog.V(2).Infof("Auto-idler: Ignoring project( %s ), already in idled state.\n", namespace)
			glog.V(2).Infof("Auto-idler: Project( %s )sync complete.\n", namespace)
			return
		}
	} else {
		glog.V(2).Infof("Auto-idler: Ignoring project( %s ), currently in force-sleep.\n", namespace)
		glog.V(2).Infof("Auto-idler: Project( %s )sync complete.\n", namespace)
		return
	}
	glog.V(2).Infof("Auto-idler: Project( %s )sync complete.\n", namespace)
}

func (idler *Idler) idleIfInactive(namespace string, pods []interface{}) error {
	for _, obj := range pods {
		pod := obj.(*cache.ResourceObject)
		if _, ok := pod.Labels[buildapi.BuildAnnotation]; ok {
			if pod.IsStarted() {
				glog.V(2).Infof("Auto-idler: Ignoring project( %s ), builder pod( %s ) found in project.\n", namespace, pod.Name)
				return nil
			}
		}
		if time.Since(pod.RunningTimes[0].Start) < idler.config.IdleQueryPeriod {
			glog.V(2).Infof("Auto-idler: Ignoring project( %s ), either pod( %s )or controller hasn't been running for complete idling cycle.\n", namespace, pod.Name)
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
					glog.V(2).Infof("Project( %s )activity below idling threshold, idling....\n", namespace)
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
		glog.V(2).Infof("Auto-idler: Ignoring project( %s ), currently in force-sleep..\n", namespace)
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
func (idler *Idler) checkIdledState(namespace string) bool {
	endpointInterface := idler.resources.KubeClient.Endpoints(namespace)
	epList, err := endpointInterface.List(kapi.ListOptions{})
	if err != nil {
		glog.Fatalf("Auto-idler: Project( %s ): %s\n", namespace, err)
	}
	for _, ep := range epList.Items {
		if _, ok := ep.ObjectMeta.Annotations[unidlingapi.IdledAtAnnotation]; ok {
			return true
		}
	}
	return false
}
