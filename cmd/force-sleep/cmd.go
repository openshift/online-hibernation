package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"
	"github.com/openshift/online-hibernation/pkg/forcesleep"
	"github.com/openshift/online-hibernation/pkg/idling"
	_ "github.com/openshift/origin/pkg/api/install"
	osclient "github.com/openshift/origin/pkg/client"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"

	"github.com/golang/glog"
	"github.com/spf13/pflag"
	"k8s.io/kubernetes/pkg/api/resource"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
)

const (
	SATokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	HawkularCA  = "/var/run/hawkular/ca.crt"
)

func main() {
	log.SetOutput(os.Stdout)
	var quota, period, sleepSyncPeriod, idleSyncPeriod, idleQueryPeriod, projectSleepPeriod time.Duration
	var workers, threshold int
	var cfgFile, excludeNamespaces, termQuota, nontermQuota, hawkularURL, metricsBindAddr string
	var hawkularInsecure, idleDryRun, sleepDryRun, inCluster, collectRuntime, collectCache bool

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
	flag.StringVar(&hawkularURL, "hawkular-url", "https://hawkular-metrics.openshift-infra.svc.cluster.local", "Hawkular url")
	flag.BoolVar(&hawkularInsecure, "insecure", true, "Use tls insecure-skip-verify to access Hawkular.")

	flag.DurationVar(&idleSyncPeriod, "idle-sync-period", 10*time.Minute, "Interval to sync project idle status")
	flag.DurationVar(&idleQueryPeriod, "idle-query-period", 30*time.Minute, "Period to compare network activity")
	flag.IntVar(&threshold, "idle-threshold", 5000, "Minimun network traffic received (bytes) to avoid auto-idling")
	flag.BoolVar(&idleDryRun, "idle-dry-run", true, "Log which projects will be auto-idled but do not idle them.")

	flag.BoolVar(&inCluster, "in-cluster", true, "If incluster, CA and token will be gathered from within pod.")
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

	//Set up kubernetes and openshift clients
	var kubeClient kclient.Interface
	var osClient osclient.Interface

	config, err := clientcmd.DefaultClientConfig(pflag.NewFlagSet("empty", pflag.ContinueOnError)).ClientConfig()
	if err != nil {
		glog.V(0).Infof("Error creating cluster config: %s", err)
		os.Exit(1)
	}
	osClient, err = osclient.New(config)
	if err != nil {
		glog.V(0).Infof("Error creating OpenShift client: %s", err)
		os.Exit(2)
	}
	kubeClient, err = kclient.New(config)
	if err != nil {
		glog.V(0).Infof("Error creating Kubernetes client: %s", err)
		os.Exit(1)
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

	cacrt := ""
	token := ""

	if inCluster {
		token = mustReadFile(SATokenFile)
		if !hawkularInsecure {
			cacrt = mustReadFile(HawkularCA)
		}
	} else {
		// If running outside of a cluster, this will have to be manually entered
		token = ""
	}

	idlerConfig := &idling.IdlerConfig{
		Exclude:            exclude,
		Url:                hawkularURL,
		IdleSyncPeriod:     idleSyncPeriod,
		IdleQueryPeriod:    idleQueryPeriod,
		CA:                 cacrt,
		Token:              token,
		SyncWorkers:        workers,
		HawkularInsecure:   hawkularInsecure,
		Threshold:          threshold,
		IdleDryRun:         idleDryRun,
		InCluster:          inCluster,
		ProjectSleepPeriod: projectSleepPeriod,
	}

	idler := idling.NewIdler(idlerConfig, factory, cache)
	idler.Run(c)
	<-c
}

// MustReadFile reads the contents of file into a string or fatally exits on
// error.
func mustReadFile(file string) string {
	if len(file) == 0 {
		glog.Fatalf("no filename specified")
	}

	path, err := filepath.Abs(file)
	if err != nil {
		glog.Fatalf("error reading %q: %s", path, err)
	}

	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		glog.Fatalf("error reading %q for file %s: %s", path, file, err)
	}

	return strings.TrimSpace(string(bytes))
}
