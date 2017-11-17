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
	"k8s.io/kubernetes/pkg/util/workqueue"
)

func init() {
	log.SetOutput(os.Stdout)
}

func TestSync(t *testing.T) {
	tests := map[string]struct {
		idleDryRun             bool
		exclude                map[string]bool
		netmap                 map[string]float64
		deploymentConfigs      []*deployapi.DeploymentConfig
		pods                   []*kapi.Pod
		services               []*kapi.Service
		replicationControllers []*kapi.ReplicationController
		resources              []*cache.ResourceObject
		expectedQueueLen       int
		expectedQueueKeys      []string
	}{
		"Single item added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens1": 1000, "somens2": 1000},
			exclude:    map[string]bool{"foo": true},
			pods: []*kapi.Pod{
				pod("somepod1", "somens1"),
				pod("somepod2", "somens2"),
			},
			services: []*kapi.Service{
				svc("somesvc1", "somens1"),
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("apoddc", "somens1"),
				dc("anotherpoddc", "somens2"),
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
			expectedQueueLen:  1,
			expectedQueueKeys: []string{"somens2"},
		},

		"2 items added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"somens3": true},
			pods: []*kapi.Pod{
				pod("somepod1", "somens1"),
				pod("somepod2", "somens2"),
			},
			services: []*kapi.Service{
				svc("somesvc1", "somens1"),
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("apoddc", "somens1"),
				dc("anotherpoddc", "somens2"),
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
			expectedQueueLen:  2,
			expectedQueueKeys: []string{"somens1", "somens2"},
		},

		"Project in excluded namespaces not added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"somens2": true},
			pods: []*kapi.Pod{
				pod("somepod2", "somens2"),
			},
			services: []*kapi.Service{
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("anotherpoddc", "somens2"),
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
			expectedQueueLen:  0,
			expectedQueueKeys: nil,
		},

		"Project with pod runningTime < IdleQueryPeriod not added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"foo": true},
			pods: []*kapi.Pod{
				pod("somepod2", "somens2"),
			},
			services: []*kapi.Service{
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("anotherpoddc", "somens2"),
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
						runningTime(time.Now().Add(-1*time.Minute),
							time.Time{}),
					},
					map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
				svcResource("5678", "somesvc2", "somens2", "2", map[string]string{"app": "anotherapp", "deploymentconfig": "anotherpoddc"}),
			},
			expectedQueueLen:  0,
			expectedQueueKeys: nil,
		},

		"2 items are scalable, but in IdleDryRun, no projects added to queue": {
			idleDryRun: true,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"somens3": true},
			pods: []*kapi.Pod{
				pod("somepod1", "somens1"),
				pod("somepod2", "somens2"),
			},
			services: []*kapi.Service{
				svc("somesvc1", "somens1"),
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("apoddc", "somens1"),
				dc("anotherpoddc", "somens2"),
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
			expectedQueueLen:  0,
			expectedQueueKeys: nil,
		},

		"No scalable resources in projects, no project added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"somens4": true},
			pods: []*kapi.Pod{
				pod("somepod1", "somens1"),
				pod("somepod2", "somens2"),
			},
			services: []*kapi.Service{
				svc("somesvc1", "somens1"),
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*kapi.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*deployapi.DeploymentConfig{
				dc("apoddc", "somens1"),
				dc("anotherpoddc", "somens2"),
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
			expectedQueueLen:  0,
			expectedQueueKeys: nil,
		},
	}

	for name, test := range tests {
		t.Logf("Testing: %s", name)
		factory := clientcmd.New(pflag.NewFlagSet("empty", pflag.ContinueOnError))
		exclude := test.exclude
		config := &IdlerConfig{
			Exclude:         exclude,
			IdleSyncPeriod:  10 * time.Minute,
			IdleQueryPeriod: 10 * time.Minute,
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
		idler := NewIdler(config, factory, fakeCache)
		for _, resource := range test.resources {
			err := idler.resources.Indexer.AddResourceObject(resource)
			if err != nil {
				t.Logf("Error: %s", err)
			}
		}
		idler.sync(test.netmap)
		assert.Equal(t, idler.queue.Len(), test.expectedQueueLen, "expected items did not match actual items in workqueue")
		nsList := examineQueue(idler.queue)
		assert.Equal(t, nsList, test.expectedQueueKeys, "unexpected project added to queue")
		idler.queue.ShutDown()
	}
}

func pod(name, namespace string) *kapi.Pod {
	return &kapi.Pod{
		TypeMeta: unversioned.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: kapi.ObjectMeta{
			Name:      name,
			Namespace: namespace,
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

func svc(name, namespace string) *kapi.Service {
	return &kapi.Service{
		TypeMeta: unversioned.TypeMeta{
			Kind: "Service",
		},
		ObjectMeta: kapi.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func dc(name, namespace string) *deployapi.DeploymentConfig {
	return &deployapi.DeploymentConfig{
		TypeMeta: unversioned.TypeMeta{
			Kind: "ReplicationController",
		},
		ObjectMeta: kapi.ObjectMeta{
			Name:      name,
			Namespace: namespace,
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

func examineQueue(queue workqueue.RateLimitingInterface) []string {
	var nsList []string
	i := queue.Len()
	for i > 0 {
		ns, _ := queue.Get()
		defer queue.Done(ns)
		nsList = append(nsList, ns.(string))
		queue.Forget(ns)
		i -= 1
	}
	return nsList
}
