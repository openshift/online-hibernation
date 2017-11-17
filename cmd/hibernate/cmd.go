package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"
	"github.com/openshift/online-hibernation/pkg/forcesleep"
	"github.com/openshift/online-hibernation/pkg/idling"
	_ "github.com/openshift/origin/pkg/api/install"
	osclient "github.com/openshift/origin/pkg/client"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"
	"github.com/prometheus/client_golang/api/prometheus"

	"github.com/golang/glog"
	"github.com/spf13/pflag"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/client/restclient"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
)

func main() {
	log.SetOutput(os.Stdout)
	var quota, period, sleepSyncPeriod, idleSyncPeriod, idleQueryPeriod, projectSleepPeriod time.Duration
	var workers, threshold int
	var cfgFile, excludeNamespaces, termQuota, nontermQuota, prometheusURL, metricsBindAddr string
	var idleDryRun, sleepDryRun, collectRuntime, collectCache bool

	flag.DurationVar(&quota, "quota", 16*time.Hour, "Maximum quota-hours allowed in period before force sleep")
	flag.DurationVar(&period, "period", 24*time.Hour, "Length of period in hours for quota consumption")
	flag.DurationVar(&sleepSyncPeriod, "sleep-sync-period", 60*time.Minute, "Interval to sync project status")
	flag.DurationVar(&projectSleepPeriod, "z", 8*time.Hour, "Length of time to apply force-sleep to projects over quota.")
	flag.BoolVar(&sleepDryRun, "sleep-dry-run", true, "Log which projects will be put into force-sleep but do not restrict them.")

	flag.IntVar(&workers, "w", 10, "Number of workers to process project sync")
	flag.StringVar(&cfgFile, "config", "", "load configuration from file")
	flag.StringVar(&excludeNamespaces, "exclude-namespace", "openshift-infra,default,openshift", "Comma-separated list of namespace to exclude in quota enforcement")

	flag.StringVar(&termQuota, "terminating", "", "Memory quota for terminating pods")
	flag.StringVar(&nontermQuota, "nonterminating", "", "Memory quota for non-terminating pods")

	flag.StringVar(&metricsBindAddr, "metricsBindAddr", ":8080", "The address on localhost serving metrics - http://localhost:port/metrics")
	flag.BoolVar(&collectRuntime, "collectRuntime", true, "Enable runtime metrics")
	flag.BoolVar(&collectCache, "collectCache", true, "Enable cache metrics")
	flag.StringVar(&prometheusURL, "prometheus-url", "https://prometheus.prometheus.svc.cluster.local", "Prometheus url")

	flag.DurationVar(&idleSyncPeriod, "idle-sync-period", 10*time.Minute, "Interval to sync project idle status")
	flag.DurationVar(&idleQueryPeriod, "idle-query-period", 30*time.Minute, "Period to compare network activity")
	flag.IntVar(&threshold, "idle-threshold", 5000, "Minimun network traffic received (bytes) to avoid auto-idling")
	flag.BoolVar(&idleDryRun, "idle-dry-run", true, "Log which projects will be auto-idled but do not idle them.")

	flag.Parse()

	tQuota, err := resource.ParseQuantity(termQuota)
	if err != nil {
		glog.V(0).Infof("Error with terminating quota: %s", err)
		os.Exit(1)
	}
	ntQuota, err := resource.ParseQuantity(nontermQuota)
	if err != nil {
		glog.V(0).Infof("Error with non-terminating quota: %s", err)
		os.Exit(1)
	}

	//Set up clients
	var kubeClient kclient.Interface
	var osClient osclient.Interface
	var prometheusClient prometheus.Client

	config, err := clientcmd.DefaultClientConfig(pflag.NewFlagSet("empty", pflag.ContinueOnError)).ClientConfig()
	if err != nil {
		glog.Fatalf("Error creating cluster config: %s", err)
	}
	osClient, err = osclient.New(config)
	if err != nil {
		glog.Fatalf("Error creating OpenShift client: %s", err)
	}
	kubeClient, err = kclient.New(config)
	if err != nil {
		glog.Fatalf("Error creating Kubernetes client: %s", err)
	}

	// @DirectXMan12 should be credited here for helping with the promCfg
	// Steal the transport from the client config -- it should have the right
	// certs, token, auth info, etc for connecting to the Prometheus OAuth proxy
	promClientConfig := *config
	promClientConfig.CAFile = "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt"
	transport, err := restclient.TransportFor(&promClientConfig)
	if err != nil {
		glog.Fatalf("Error creating cluster config: %s", err)
	}

	promCfg := prometheus.Config{
		Address: prometheusURL,
		// TODO: technically dubious
		Transport: transport.(prometheus.CancelableTransport),
	}

	prometheusClient, err = prometheus.New(promCfg)
	if err != nil {
		glog.Fatalf("Error creating Prometheus client: %s", err)
	}

	factory := clientcmd.New(pflag.NewFlagSet("empty", pflag.ContinueOnError))

	namespaces := strings.Split(excludeNamespaces, ",")
	exclude := make(map[string]bool)
	for _, name := range namespaces {
		exclude[name] = true
	}
	c := make(chan struct{})

	//Cache is a shared object that both Sleeper and Idler will hold a reference to and interact with
	cache := cache.NewCache(osClient, kubeClient, factory, exclude)
	cache.Run(c)

	sleeperConfig := &forcesleep.SleeperConfig{
		Quota:              quota,
		Period:             period,
		SleepSyncPeriod:    sleepSyncPeriod,
		SyncWorkers:        workers,
		Exclude:            exclude,
		ProjectSleepPeriod: projectSleepPeriod,
		TermQuota:          tQuota,
		NonTermQuota:       ntQuota,
		DryRun:             sleepDryRun,
	}

	sleeper := forcesleep.NewSleeper(sleeperConfig, factory, cache)

	// Spawn metrics server
	go func() {
		metricsConfig := forcesleep.MetricsConfig{
			BindAddr:       metricsBindAddr,
			CollectRuntime: collectRuntime,
			CollectCache:   collectCache,
		}

		server := forcesleep.MetricsServer{
			Config:     metricsConfig,
			Controller: sleeper,
		}
		err := server.Serve()
		if err != nil {
			glog.Errorf("Error running metrics server: %s", err)
		}
	}()

	sleeper.Run(c)

	idlerConfig := &idling.IdlerConfig{
		Exclude:            exclude,
		PrometheusClient:   prometheusClient,
		IdleSyncPeriod:     idleSyncPeriod,
		IdleQueryPeriod:    idleQueryPeriod,
		SyncWorkers:        workers,
		Threshold:          threshold,
		IdleDryRun:         idleDryRun,
		ProjectSleepPeriod: projectSleepPeriod,
	}

	idler := idling.NewIdler(idlerConfig, factory, cache)
	idler.Run(c)
	<-c
}
