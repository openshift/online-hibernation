package idling

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"

	"github.com/openshift/origin/pkg/client/testclient"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"
	deployapi "github.com/openshift/origin/pkg/deploy/api"
	"github.com/stretchr/testify/assert"

	"github.com/spf13/pflag"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/api/unversioned"
	ktestclient "k8s.io/kubernetes/pkg/client/unversioned/testclient"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/types"
)

func init() {
	log.SetOutput(os.Stdout)
}

func TestSync(t *testing.T) {
	tests := []struct {
		name                   string
		idleDryRun             bool
		exclude                map[string]bool
		netmap                 map[string]float64
		deploymentConfigs      []*deployapi.DeploymentConfig
		pods                   []*kapi.Pod
		services               []*kapi.Service
		replicationControllers []*kapi.ReplicationController
		resources              []*cache.ResourceObject
		items_in_queue         int
		queue_key              string
	}{
		{
			name:       "Item is added to queue",
			idleDryRun: false,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"somens1": true},
			pods: []*kapi.Pod{
				pod("somepod1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				pod("somepod2", "somens2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			services: []*kapi.Service{
				svc("somesvc1", "somens1", map[string]string{"foo": "bar"}),
				svc("somesvc2", "somens2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("apoddc", "somens1", map[string]string{"app": "anapp"}),
				dc("anotherpoddc", "somens2", map[string]string{"app": "anotherapp"}),
			},
			resources: []*cache.ResourceObject{
				projectResource("somens1", false),
				projectResource("somens2", false),
				rcResource("somerc1", "somerc1", "somens1", "1", "apoddc", []*cache.RunningTime{
					runningTime(time.Now().Add(-1*time.Hour),
						time.Time{}),
				}),
				rcResource("somerc2", "somerc2", "somens2", "1", "anotherpoddc", []*cache.RunningTime{
					runningTime(time.Now().Add(-1*time.Hour),
						time.Time{}),
				}),
				podResource("somepod1", "somepod1", "somens1", "1",
					resource.MustParse("500M"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-1*time.Hour),
							time.Time{}),
					},
					map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				podResource("somepod2", "somepod2", "somens2", "2",
					resource.MustParse("1G"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-1*time.Hour),
							time.Time{}),
					},
					map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
				svcResource("1234", "somesvc1", "somens1", "1", map[string]string{"foo": "bar"}),
				svcResource("5678", "somesvc2", "somens2", "2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			items_in_queue: 1,
			queue_key:      "somens2",
		},
		{
			name:       "Projects in excluded namespaces are not added to queue",
			idleDryRun: false,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"somens2": true},
			pods: []*kapi.Pod{
				pod("somepod2", "somens2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			services: []*kapi.Service{
				svc("somesvc2", "somens2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("anotherpoddc", "somens2", map[string]string{"app": "anotherapp"}),
			},
			resources: []*cache.ResourceObject{
				projectResource("somens2", false),
				rcResource("somerc2", "somerc2", "somens2", "1", "anotherpoddc", []*cache.RunningTime{
					runningTime(time.Now().Add(-1*time.Hour),
						time.Time{}),
				}),
				podResource("somepod2", "somepod2", "somens2", "2",
					resource.MustParse("1G"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-1*time.Hour),
							time.Time{}),
					},
					map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
				svcResource("5678", "somesvc2", "somens2", "2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			items_in_queue: 0,
			queue_key:      "none",
		},
		{
			name:       "2 items should be  added to queue",
			idleDryRun: false,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"": true},
			pods: []*kapi.Pod{
				pod("somepod1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				pod("somepod2", "somens2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			services: []*kapi.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				svc("somesvc2", "somens2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("apoddc", "somens1", map[string]string{"app": "anapp"}),
				dc("anotherpoddc", "somens2", map[string]string{"app": "anotherapp"}),
			},
			resources: []*cache.ResourceObject{
				projectResource("somens1", false),
				projectResource("somens2", false),
				rcResource("somerc1", "somerc1", "somens1", "1", "apoddc", []*cache.RunningTime{
					runningTime(time.Now().Add(-1*time.Hour),
						time.Time{}),
				}),
				rcResource("somerc2", "somerc2", "somens2", "1", "anotherpoddc", []*cache.RunningTime{
					runningTime(time.Now().Add(-1*time.Hour),
						time.Time{}),
				}),
				podResource("somepod1", "somepod1", "somens1", "1",
					resource.MustParse("500M"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-1*time.Hour),
							time.Time{}),
					},
					map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				podResource("somepod2", "somepod2", "somens2", "2",
					resource.MustParse("1G"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-1*time.Hour),
							time.Time{}),
					},
					map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
				svcResource("1234", "somesvc1", "somens1", "1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				svcResource("5678", "somesvc2", "somens2", "2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			items_in_queue: 2,
			queue_key:      "somens1",
		},
		{
			name:       "2 items are scalable, but in IdleDryRun, queue should have 0 items",
			idleDryRun: true,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"": true},
			pods: []*kapi.Pod{
				pod("somepod1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				pod("somepod2", "somens2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			services: []*kapi.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				svc("somesvc2", "somens2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("apoddc", "somens1", map[string]string{"app": "anapp"}),
				dc("anotherpoddc", "somens2", map[string]string{"app": "anotherapp"}),
			},
			resources: []*cache.ResourceObject{
				projectResource("somens1", false),
				projectResource("somens2", false),
				rcResource("somerc1", "somerc1", "somens1", "1", "apoddc", []*cache.RunningTime{
					runningTime(time.Now().Add(-1*time.Hour),
						time.Time{}),
				}),
				rcResource("somerc2", "somerc2", "somens2", "1", "anotherpoddc", []*cache.RunningTime{
					runningTime(time.Now().Add(-1*time.Hour),
						time.Time{}),
				}),
				podResource("somepod1", "somepod1", "somens1", "1",
					resource.MustParse("500M"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-1*time.Hour),
							time.Time{}),
					},
					map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				podResource("somepod2", "somepod2", "somens2", "2",
					resource.MustParse("1G"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-1*time.Hour),
							time.Time{}),
					},
					map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
				svcResource("1234", "somesvc1", "somens1", "1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				svcResource("5678", "somesvc2", "somens2", "2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			items_in_queue: 0,
			queue_key:      "none",
		},
		{
			name:       "No scalable resources in projects, queue should have 0 items",
			idleDryRun: false,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"": true},
			pods: []*kapi.Pod{
				pod("somepod1", "somens1", map[string]string{"foo": "bar"}),
				pod("somepod2", "somens2", map[string]string{"cheese": "sandwich"}),
			},
			services: []*kapi.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				svc("somesvc2", "somens2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("apoddc", "somens1", map[string]string{"app": "anapp"}),
				dc("anotherpoddc", "somens2", map[string]string{"app": "anotherapp"}),
			},
			resources: []*cache.ResourceObject{
				projectResource("somens1", false),
				projectResource("somens2", false),
				rcResource("somerc1", "somerc1", "somens1", "1", "apoddc", []*cache.RunningTime{
					runningTime(time.Now().Add(-1*time.Hour),
						time.Time{}),
				}),
				rcResource("somerc2", "somerc2", "somens2", "1", "anotherpoddc", []*cache.RunningTime{
					runningTime(time.Now().Add(-1*time.Hour),
						time.Time{}),
				}),
				podResource("somepod1", "somepod1", "somens1", "1",
					resource.MustParse("500M"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-1*time.Hour),
							time.Time{}),
					},
					map[string]string{"foo": "bar"}),
				podResource("somepod2", "somepod2", "somens2", "2",
					resource.MustParse("1G"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-1*time.Hour),
							time.Time{}),
					},
					map[string]string{"cheese": "sandwich"}),
				svcResource("1234", "somesvc1", "somens1", "1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				svcResource("5678", "somesvc2", "somens2", "2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			items_in_queue: 0,
			queue_key:      "none",
		},
	}
	for _, test := range tests {
		t.Logf("Testing: %s", test.name)
		prometheus := fakePrometheusMetrics(test.netmap)
		factory := clientcmd.New(pflag.NewFlagSet("empty", pflag.ContinueOnError))
		exclude := test.exclude
		config := &IdlerConfig{
			Exclude:         exclude,
			IdleSyncPeriod:  10 * time.Second,
			IdleQueryPeriod: 10 * time.Second,
			Threshold:       2000,
			SyncWorkers:     2,
			IdleDryRun:      test.idleDryRun,
		}
		fakeOClient := &testclient.Fake{}
		fakeClient := &ktestclient.Fake{}
		fakeOClient.AddReactor("list", "deploymentconfigs", func(action ktestclient.Action) (handled bool, ret runtime.Object, err error) {
			list := &deployapi.DeploymentConfigList{}
			for i := range test.deploymentConfigs {
				list.Items = append(list.Items, *test.deploymentConfigs[i])
			}
			return true, list, nil
		})

		fakeClient.AddReactor("list", "services", func(action ktestclient.Action) (handled bool, resp runtime.Object, err error) {
			list := &kapi.ServiceList{}
			for i := range test.services {
				list.Items = append(list.Items, *test.services[i])
			}
			return true, list, nil
		})

		fakeClient.AddReactor("list", "replicationcontrollers", func(action ktestclient.Action) (handled bool, resp runtime.Object, err error) {
			list := &kapi.ReplicationControllerList{}
			for i := range test.replicationControllers {
				list.Items = append(list.Items, *test.replicationControllers[i])
			}
			return true, list, nil
		})

		fakeClient.AddReactor("list", "pods", func(action ktestclient.Action) (handled bool, resp runtime.Object, err error) {
			list := &kapi.PodList{}
			for i := range test.pods {
				list.Items = append(list.Items, *test.pods[i])
			}
			return true, list, nil
		})

		fakeCache := cache.NewCache(fakeOClient, fakeClient, factory, exclude)
		idler := NewIdler(config, factory, fakeCache, prometheus)
		for _, resource := range test.resources {
			err := idler.resources.Indexer.AddResourceObject(resource)
			if err != nil {
				t.Logf("Error: %s", err)
			}
		}
		idler.sync()
		assert.Equal(t, idler.queue.Len(), test.items_in_queue, "expected items did not match actual items in workqueue")
		if idler.queue.Len() == 1 {
			ns, _ := idler.queue.Get()
			assert.Equal(t, ns.(string), test.queue_key, "unexpected project added to queue")
			idler.queue.Done(ns)
		}

		idler.queue.ShutDown()
	}
}

type FakePrometheusMetrics struct {
	netmap map[string]float64
}

func fakePrometheusMetrics(netmap map[string]float64) *PrometheusMetrics {
	prometheus := &FakePrometheusMetrics{
		netmap,
	}
	return prometheus
}

func (pm *FakePrometheusMetrics) GetNetworkActivity() map[string]float64 {
	return pm.netmap
}

func pod(name, namespace string, labels map[string]string) *kapi.Pod {
	return &kapi.Pod{
		TypeMeta: unversioned.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: kapi.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
	}
}

func rc(name, namespace string) *kapi.ReplicationController {
	return &kapi.ReplicationController{
		TypeMeta: unversioned.TypeMeta{
			Kind: "ReplicationController",
		},
		ObjectMeta: kapi.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func svc(name, namespace string, selectors map[string]string) *kapi.Service {
	return &kapi.Service{
		TypeMeta: unversioned.TypeMeta{
			Kind: "Service",
		},
		ObjectMeta: kapi.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: kapi.ServiceSpec{
			Selector: selectors,
		},
	}
}

func dc(name, namespace string, labels map[string]string) *deployapi.DeploymentConfig {
	return &deployapi.DeploymentConfig{
		TypeMeta: unversioned.TypeMeta{
			Kind: "ReplicationController",
		},
		ObjectMeta: kapi.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
	}
}

func podResource(uid, name, namespace, resourceVersion string, request resource.Quantity, rt []*cache.RunningTime, labels map[string]string) *cache.ResourceObject {
	return &cache.ResourceObject{
		UID:             types.UID(uid),
		Name:            name,
		Namespace:       namespace,
		Kind:            cache.PodKind,
		ResourceVersion: resourceVersion,
		MemoryRequested: request,
		RunningTimes:    rt,
		Terminating:     false,
		Labels:          labels,
	}
}

func rcResource(uid, name, namespace, resourceVersion, dc string, rt []*cache.RunningTime) *cache.ResourceObject {
	return &cache.ResourceObject{
		UID:              types.UID(uid),
		Name:             name,
		Namespace:        namespace,
		Kind:             cache.RCKind,
		ResourceVersion:  resourceVersion,
		DeploymentConfig: dc,
		RunningTimes:     rt,
	}
}

func svcResource(uid string, name string, namespace string, resourceVersion string, selectors map[string]string) *cache.ResourceObject {
	return &cache.ResourceObject{
		UID:             types.UID(uid),
		Name:            name,
		Namespace:       namespace,
		Kind:            cache.ServiceKind,
		ResourceVersion: resourceVersion,
		Selectors:       selectors,
	}
}

func projectResource(name string, isAsleep bool) *cache.ResourceObject {
	return &cache.ResourceObject{
		UID:       types.UID(name),
		Name:      name,
		Namespace: name,
		Kind:      cache.ProjectKind,
		IsAsleep:  isAsleep,
	}
}

func runningTime(start, end time.Time) *cache.RunningTime {
	return &cache.RunningTime{
		Start: start,
		End:   end,
	}
}
