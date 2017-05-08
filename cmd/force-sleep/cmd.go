package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	osclient "github.com/openshift/origin/pkg/client"

	"github.com/openshift/online/force-sleep/pkg/cache"
	"github.com/openshift/online/force-sleep/pkg/forcesleep"
	_ "github.com/openshift/origin/pkg/api/install"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"

	"github.com/golang/glog"
	"github.com/spf13/pflag"
	"k8s.io/kubernetes/pkg/api/resource"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
)

func main() {
	log.SetOutput(os.Stdout)
	var quota, period, syncPeriod, projectSleepPeriod time.Duration
	var workers int
	var excludeNamespaces, termQuota, nontermQuota string
	flag.DurationVar(&quota, "q", 16*time.Hour, "Maximum quota-hours allowed in period before force sleep")
	flag.DurationVar(&period, "p", 24*time.Hour, "Length of period in hours for quota consumption")
	flag.DurationVar(&syncPeriod, "s", 60*time.Minute, "Interval to sync project status")
	flag.DurationVar(&projectSleepPeriod, "z", 8*time.Hour, "Length of time to apply force-sleep to projects over quota.")
	flag.IntVar(&workers, "w", 10, "Number of workers to process project sync")
	flag.StringVar(&excludeNamespaces, "n", "", "Comma-separated list of namespace to exclude in quota enforcement")
	flag.StringVar(&termQuota, "terminating", "", "Memory quota for terminating pods")
	flag.StringVar(&nontermQuota, "nonterminating", "", "Memory quota for non-terminating pods")
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

	/*
	 * Set up kubernetes and openshift clients
	 */
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
	f := clientcmd.New(pflag.NewFlagSet("empty", pflag.ContinueOnError))

	/*
	 * Cache is a shared object that both Sleeper and Idler will hold a reference to and interact with
	 */
	cache := cache.NewCache(osClient, kubeClient, f)

	// Create Sleeper
	// TODO create Idler with similar setup and run in another thread
	namespaces := strings.Split(excludeNamespaces, ",")
	exclude := make(map[string]bool)
	for _, name := range namespaces {
		exclude[name] = true
	}
	sleeperConfig := &forcesleep.SleeperConfig{
		Quota:              quota,
		Period:             period,
		SyncPeriod:         syncPeriod,
		SyncWorkers:        workers,
		Exclude:            exclude,
		ProjectSleepPeriod: projectSleepPeriod,
		TermQuota:          tQuota,
		NonTermQuota:       ntQuota,
	}
	sleeper := forcesleep.NewSleeper(osClient, kubeClient, sleeperConfig, f, cache)
	c := make(chan struct{})
	sleeper.Run(c)
	<-c
}
