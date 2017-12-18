package idling

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"

	appsv1 "github.com/openshift/api/apps/v1"
	fakeoclientset "github.com/openshift/client-go/apps/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakekclientset "k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/workqueue"
)

func init() {
	log.SetOutput(os.Stdout)
}

func TestSync(t *testing.T) {
	tests := map[string]struct {
		idleDryRun             bool
		exclude                map[string]bool
		netmap                 map[string]float64
		deploymentConfigs      []*appsv1.DeploymentConfig
		pods                   []*corev1.Pod
		services               []*corev1.Service
		replicationControllers []*corev1.ReplicationController
		resources              []*cache.ResourceObject
		expectedQueueLen       int
		expectedQueueKeys      []string
	}{
		"Single item added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens1": 1000},
			exclude:    map[string]bool{"somens3": true},
			pods: []*corev1.Pod{
				pod("somepod1", "somens1"),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1"),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1"),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "somens1"),
			},
			resources: []*cache.ResourceObject{
				projectResource("somens1", false),
				rcResource("somerc1", "somerc1", "somens1", "1", "apoddc", []*cache.RunningTime{
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
				svcResource("1234", "somesvc1", "somens1", "1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
			},
			expectedQueueLen:  1,
			expectedQueueKeys: []string{"somens1"},
		},

		"2 items added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens2": 1000, "somens1": 1000},
			exclude:    map[string]bool{"somens3": true},
			pods: []*corev1.Pod{
				pod("somepod1", "somens1"),
				pod("somepod2", "somens2"),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1"),
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
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
			pods: []*corev1.Pod{
				pod("somepod2", "somens2"),
			},
			services: []*corev1.Service{
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
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
			pods: []*corev1.Pod{
				pod("somepod2", "somens2"),
			},
			services: []*corev1.Service{
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
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
			pods: []*corev1.Pod{
				pod("somepod1", "somens1"),
				pod("somepod2", "somens2"),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1"),
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
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
			pods: []*corev1.Pod{
				pod("somepod1", "somens1"),
				pod("somepod2", "somens2"),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1"),
				svc("somesvc2", "somens2"),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1"),
				rc("somerc2", "somens2"),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
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
		exclude := test.exclude
		config := &IdlerConfig{
			Exclude:         exclude,
			IdleSyncPeriod:  10 * time.Minute,
			IdleQueryPeriod: 10 * time.Minute,
			Threshold:       2000,
			SyncWorkers:     2,
			IdleDryRun:      test.idleDryRun,
		}

		clientConfig := &restclient.Config{
			Host: "127.0.0.1",
			ContentConfig: restclient.ContentConfig{GroupVersion: &corev1.SchemeGroupVersion,
				NegotiatedSerializer: cache.Codecs},
		}
		fakeOClient := fakeoclientset.NewSimpleClientset()
		fakeClient := &fakekclientset.Clientset{}

		fakeOClient.AddReactor("list", "deploymentconfigs", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
			list := &appsv1.DeploymentConfigList{}
			for i := range test.deploymentConfigs {
				list.Items = append(list.Items, *test.deploymentConfigs[i])
			}
			return true, list, nil
		})

		fakeClient.AddReactor("list", "services", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
			list := &corev1.ServiceList{}
			for i := range test.services {
				list.Items = append(list.Items, *test.services[i])
			}
			return true, list, nil
		})

		fakeClient.AddReactor("list", "replicationcontrollers", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
			list := &corev1.ReplicationControllerList{}
			for i := range test.replicationControllers {
				list.Items = append(list.Items, *test.replicationControllers[i])
			}
			return true, list, nil
		})

		fakeClient.AddReactor("list", "pods", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
			list := &corev1.PodList{}
			for i := range test.pods {
				list.Items = append(list.Items, *test.pods[i])
			}
			return true, list, nil
		})

		fakeCache := cache.NewCache(fakeOClient, fakeClient, clientConfig, nil, exclude)
		idler := NewIdler(config, fakeCache)
		for _, resource := range test.resources {
			err := idler.resources.Indexer.AddResourceObject(resource)
			if err != nil {
				t.Logf("Error: %s", err)
			}
		}
		idler.sync(test.netmap)
		assert.Equal(t, idler.queue.Len(), test.expectedQueueLen, "expected items did not match actual items in workqueue")
		nsList := examineQueue(idler.queue)
		assert.Equal(t, nsList, test.expectedQueueKeys, "unexpected queue contents")
		idler.queue.ShutDown()
	}
}

func pod(name, namespace string) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func rc(name, namespace string) *corev1.ReplicationController {
	return &corev1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind: "ReplicationController",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func svc(name, namespace string) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind: "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func dc(name, namespace string) *appsv1.DeploymentConfig {
	return &appsv1.DeploymentConfig{
		TypeMeta: metav1.TypeMeta{
			Kind: "ReplicationController",
		},
		ObjectMeta: metav1.ObjectMeta{
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
		UID:             types.UID(uid),
		Name:            name,
		Namespace:       namespace,
		Kind:            cache.RCKind,
		ResourceVersion: resourceVersion,
		RunningTimes:    rt,
	}
}

func svcResource(uid string, name string, namespace string, resourceVersion string, selectors map[string]string) *cache.ResourceObject {
	selector, _ := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: selectors})
	return &cache.ResourceObject{
		UID:             types.UID(uid),
		Name:            name,
		Namespace:       namespace,
		Kind:            cache.ServiceKind,
		ResourceVersion: resourceVersion,
		Selectors:       selector,
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
