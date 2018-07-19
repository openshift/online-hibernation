package autoidling

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"

	appsv1 "github.com/openshift/api/apps/v1"
	fakeoclientset "github.com/openshift/client-go/apps/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	extv1beta1 "k8s.io/api/extensions/v1beta1"

	fakeidlersclient "github.com/openshift/service-idler/pkg/client/clientset/versioned/fake"
	iclient "github.com/openshift/service-idler/pkg/client/clientset/versioned/typed/idling/v1alpha2"
	kappsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakedisco "k8s.io/client-go/discovery/fake"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	kclient "k8s.io/client-go/kubernetes"
	fakekclientset "k8s.io/client-go/kubernetes/fake"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/scale"
	ktesting "k8s.io/client-go/testing"
	kcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

func makeServiceLister(services []*corev1.Service) listerscorev1.ServiceLister {
	c := kcache.NewIndexer(kcache.MetaNamespaceKeyFunc, kcache.Indexers{kcache.NamespaceIndex: kcache.MetaNamespaceIndexFunc})
	for _, svc := range services {
		if err := c.Add(svc); err != nil {
			fmt.Printf("%s", err)
			return nil
		}
	}
	lister := listerscorev1.NewServiceLister(c)
	return lister
}

func makeNamespaceLister(namespaces []*corev1.Namespace) listerscorev1.NamespaceLister {
	c := kcache.NewIndexer(kcache.MetaNamespaceKeyFunc, kcache.Indexers{kcache.NamespaceIndex: kcache.MetaNamespaceIndexFunc})
	for _, ns := range namespaces {
		if err := c.Add(ns); err != nil {
			fmt.Printf("%s", err)
			return nil
		}
	}
	lister := listerscorev1.NewNamespaceLister(c)
	return lister
}

func makePodLister(pods []*corev1.Pod) listerscorev1.PodLister {
	c := kcache.NewIndexer(kcache.MetaNamespaceKeyFunc, kcache.Indexers{kcache.NamespaceIndex: kcache.MetaNamespaceIndexFunc})
	for _, pod := range pods {
		if err := c.Add(pod); err != nil {
			fmt.Printf("%s", err)
			return nil
		}
	}
	lister := listerscorev1.NewPodLister(c)
	return lister
}

func init() {
	log.SetOutput(os.Stdout)
}

func NewFakeResourceStore(pods []*corev1.Pod, projects []*corev1.Namespace, services []*corev1.Service, kc kclient.Interface, ic iclient.IdlersGetter, skr scale.ScaleKindResolver) *cache.ResourceStore {
	resourceStore := &cache.ResourceStore{
		KubeClient:        kc,
		PodList:           makePodLister(pods),
		ProjectList:       makeNamespaceLister(projects),
		ServiceList:       makeServiceLister(services),
		ClientPool:        &fakedynamic.FakeClientPool{},
		IdlersClient:      ic,
		ScaleKindResolver: skr,
	}
	return resourceStore
}

func containerListForMemory(memory string) []corev1.Container {
	var containerList []corev1.Container
	quantity := resource.MustParse(memory)
	limits := make(map[corev1.ResourceName]resource.Quantity)
	limits["memory"] = quantity
	aContainer := corev1.Container{Name: "aname", Resources: corev1.ResourceRequirements{Limits: limits}}
	containerList = append(containerList, aContainer)
	return containerList
}

func TestSync(t *testing.T) {
	tests := map[string]struct {
		idleDryRun             bool
		netmap                 map[string]float64
		pods                   []*corev1.Pod
		projects               []*corev1.Namespace
		services               []*corev1.Service
		replicationControllers []*corev1.ReplicationController
		statefulSets           []*kappsv1.StatefulSet
		replicaSets            []*kappsv1.ReplicaSet
		deployments            []*kappsv1.Deployment
		deploymentConfigs      []*appsv1.DeploymentConfig
		expectedQueueLen       int
		expectedQueueKeys      []string
	}{
		"Single item added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens1": 1000},
			pods: []*corev1.Pod{
				pod("somepod1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1", "apoddc", 1, map[string]string{}, map[string]string{}),
			},
			// NOTE: int32 not *int32
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "somens1", 1, map[string]string{}, map[string]string{}),
			},
			projects: []*corev1.Namespace{
				activeNamespace("somens1", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				terminatingNamespace("someDeletedNS", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			expectedQueueLen:  1,
			expectedQueueKeys: []string{"somens1"},
		},

		"Single item added to queue with replicaset": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens1": 1000},
			pods: []*corev1.Pod{
				pod("somepod1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
			},
			replicaSets: []*kappsv1.ReplicaSet{
				rs("somers1", "somens1", 1, map[string]string{}, map[string]string{}),
			},
			projects: []*corev1.Namespace{
				activeNamespace("somens1", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				terminatingNamespace("someDeletedNS", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			expectedQueueLen:  1,
			expectedQueueKeys: []string{"somens1"},
		},

		"netmap len=0, no panic": {
			idleDryRun: false,
			netmap:     map[string]float64{},
			pods: []*corev1.Pod{
				pod("somepod1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1", "apoddc", 1, map[string]string{}, map[string]string{}),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "somens1", 1, map[string]string{}, map[string]string{}),
			},
			projects: []*corev1.Namespace{
				activeNamespace("somens1", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				terminatingNamespace("someDeletedNS", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			expectedQueueLen:  0,
			expectedQueueKeys: nil,
		},

		"2 items added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens1": 1000, "somens2": 1000},
			pods: []*corev1.Pod{
				pod("somepod1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
				pod("somepod2", "somens2", map[string]string{"app2": "anapp2", "deploymentconfig2": "apoddc2"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				svc("somesvc2", "somens2", map[string]string{"app2": "anapp2", "deploymentconfig2": "apoddc2"}),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1", "apoddc", 1, map[string]string{}, map[string]string{}),
				rc("somerc2", "somens2", "apoddc2", 1, map[string]string{}, map[string]string{}),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "somens1", 1, map[string]string{}, map[string]string{}),
				dc("apoddc2", "somens2", 1, map[string]string{}, map[string]string{}),
			},
			projects: []*corev1.Namespace{
				activeNamespace("somens1", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				activeNamespace("somens2", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				terminatingNamespace("someDeletedNS", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			expectedQueueLen:  2,
			expectedQueueKeys: []string{"somens1", "somens2"},
		},

		"2 scalable items, but IdleDryRun=true, nothing added to queue": {
			idleDryRun: true,
			netmap:     map[string]float64{"somens1": 1000, "somens2": 1000},
			pods: []*corev1.Pod{
				pod("somepod1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
				pod("somepod2", "somens2", map[string]string{"app2": "anapp2", "deploymentconfig2": "apoddc2"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				svc("somesvc2", "somens2", map[string]string{"app2": "anapp2", "deploymentconfig2": "apoddc2"}),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1", "apoddc", 1, map[string]string{}, map[string]string{}),
				rc("somerc2", "somens2", "apoddc2", 1, map[string]string{}, map[string]string{}),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "somens1", 1, map[string]string{}, map[string]string{}),
				dc("apoddc2", "somens2", 1, map[string]string{}, map[string]string{}),
			},
			projects: []*corev1.Namespace{
				activeNamespace("somens1", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				activeNamespace("somens2", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				terminatingNamespace("someDeletedNS", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			expectedQueueLen:  0,
			expectedQueueKeys: nil,
		},

		"No scalable items, nothing added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens1": 1000, "somens2": 1000},
			pods: []*corev1.Pod{
				pod("somepod1", "somens1", map[string]string{"blah": "blah", "not-the-same": "apoddc"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
				pod("somepod2", "somens2", map[string]string{"not": "the", "same": "as-svc-selector"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
				svc("somesvc2", "somens2", map[string]string{"app2": "anapp2", "deploymentconfig2": "apoddc2"}),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1", "apoddc", 1, map[string]string{}, map[string]string{}),
				rc("somerc2", "somens2", "apoddc2", 1, map[string]string{}, map[string]string{}),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "somens1", 1, map[string]string{}, map[string]string{}),
				dc("apoddc2", "somens2", 1, map[string]string{}, map[string]string{}),
			},
			projects: []*corev1.Namespace{
				activeNamespace("somens1", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				activeNamespace("somens2", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				terminatingNamespace("someDeletedNS", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			expectedQueueLen:  0,
			expectedQueueKeys: nil,
		},

		"Project with pod running time < IdleQueryPeriod not added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens1": 1000},
			pods: []*corev1.Pod{
				pod("somepod1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-9*time.Minute)), corev1.PodRunning),
			},
			services: []*corev1.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1", "apoddc", 1, map[string]string{}, map[string]string{}),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "somens1", 1, map[string]string{}, map[string]string{}),
			},
			projects: []*corev1.Namespace{
				activeNamespace("somens1", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				terminatingNamespace("someDeletedNS", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			expectedQueueLen:  0,
			expectedQueueKeys: nil,
		},
	}

	for name, test := range tests {
		t.Logf("Testing: %s", name)
		config := &AutoIdlerConfig{
			IdleSyncPeriod:  10 * time.Minute,
			IdleQueryPeriod: 10 * time.Minute,
			Threshold:       2000,
			SyncWorkers:     2,
			IdleDryRun:      test.idleDryRun,
		}
		fakeOClient := fakeoclientset.Clientset{}
		fakeClient := fakekclientset.Clientset{}
		kc := &fakeClient
		fakeIdlersClient := fakeidlersclient.Clientset{}
		ic := fakeIdlersClient.IdlingV1alpha2()

		// Borrowed this from k8s.io/client-go/scale/client_test.go
		fakeDiscoveryClient := &fakedisco.FakeDiscovery{Fake: &ktesting.Fake{}}
		fakeDiscoveryClient.Resources = []*metav1.APIResourceList{
			{
				GroupVersion: corev1.SchemeGroupVersion.String(),
				APIResources: []metav1.APIResource{
					{Name: "pods", Namespaced: true, Kind: "Pod"},
					{Name: "replicationcontrollers", Namespaced: true, Kind: "ReplicationController"},
					{Name: "replicationcontrollers/scale", Namespaced: true, Kind: "Scale", Group: "autoscaling", Version: "v1"},
				},
			},
			{
				GroupVersion: extv1beta1.SchemeGroupVersion.String(),
				APIResources: []metav1.APIResource{
					{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
					{Name: "replicasets/scale", Namespaced: true, Kind: "Scale"},
				},
			},
			{
				GroupVersion: appsv1beta2.SchemeGroupVersion.String(),
				APIResources: []metav1.APIResource{
					{Name: "deployments", Namespaced: true, Kind: "Deployment"},
					{Name: "deployments/scale", Namespaced: true, Kind: "Scale", Group: "apps", Version: "v1beta2"},
				},
			},
			{
				GroupVersion: appsv1beta1.SchemeGroupVersion.String(),
				APIResources: []metav1.APIResource{
					{Name: "statefulsets", Namespaced: true, Kind: "StatefulSet"},
					{Name: "statefulsets/scale", Namespaced: true, Kind: "Scale", Group: "apps", Version: "v1beta1"},
				},
			},
			// Clearly, this was written by @DirectXMan12
			// test a resource that doesn't exist anywere to make sure we're not accidentally depending
			// on a static RESTMapper anywhere.
			{
				GroupVersion: "cheese.testing.k8s.io/v27alpha15",
				APIResources: []metav1.APIResource{
					{Name: "cheddars", Namespaced: true, Kind: "Cheddar"},
					{Name: "cheddars/scale", Namespaced: true, Kind: "Scale", Group: "extensions", Version: "v1beta1"},
				},
			},
		}

		resolver := scale.NewDiscoveryScaleKindResolver(fakeDiscoveryClient)
		resources := NewFakeResourceStore(test.pods, test.projects, test.services, kc, ic, resolver)

		fakeOClient.AddReactor("list", "deploymentconfigs", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
			list := &appsv1.DeploymentConfigList{}
			for i := range test.deploymentConfigs {
				list.Items = append(list.Items, *test.deploymentConfigs[i])
			}
			return true, list, nil
		})

		fakeClient.AddReactor("list", "namespaces", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
			list := &corev1.NamespaceList{}
			for i := range test.projects {
				list.Items = append(list.Items, *test.projects[i])
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

		fakeClient.AddReactor("list", "replicasets", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
			list := &kappsv1.ReplicaSetList{}
			for i := range test.replicaSets {
				list.Items = append(list.Items, *test.replicaSets[i])
			}
			return true, list, nil
		})

		fakeClient.AddReactor("list", "statefulsets", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
			list := &kappsv1.StatefulSetList{}
			for i := range test.statefulSets {
				list.Items = append(list.Items, *test.statefulSets[i])
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

		fakeClient.AddReactor("get", "resourcequotas", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
			return true, nil, nil
		})

		idler := NewAutoIdler(config, resources)
		idler.sync(test.netmap)
		assert.Equal(t, idler.queue.Len(), test.expectedQueueLen, "expected items did not match actual items in workqueue")
		nsList := examineQueue(idler.queue)
		assert.Equal(t, nsList, test.expectedQueueKeys, "unexpected queue contents")
		idler.queue.ShutDown()
	}
}

func resourceQuota(name string) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				"pods": *resource.NewMilliQuantity(0, resource.DecimalSI),
			},
		},
	}
}

func pod(name, namespace string, labels map[string]string, activeDeadlineSeconds *int64, memory string, startTime metav1.Time, phase corev1.PodPhase) *corev1.Pod {
	containerList := containerListForMemory(memory)
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:         corev1.RestartPolicyAlways,
			ActiveDeadlineSeconds: activeDeadlineSeconds,
			Containers:            containerList,
		},
		Status: corev1.PodStatus{
			StartTime: &startTime,
			Phase:     phase,
		},
	}
}

func deletedPod(name, namespace string, labels map[string]string, activeDeadlineSeconds *int64, memory string, startTime metav1.Time, phase corev1.PodPhase, deltime metav1.Time) *corev1.Pod {
	containerList := containerListForMemory(memory)
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			Labels:            labels,
			DeletionTimestamp: &deltime,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:         corev1.RestartPolicyAlways,
			ActiveDeadlineSeconds: activeDeadlineSeconds,
			Containers:            containerList,
		},
		Status: corev1.PodStatus{
			StartTime: &startTime,
			Phase:     phase,
		},
	}
}

// NOTE: int32 not *int32
func dc(name, namespace string, replicas int32, labels, annotations map[string]string) *appsv1.DeploymentConfig {
	return &appsv1.DeploymentConfig{
		TypeMeta: metav1.TypeMeta{
			Kind: "DeploymentConfig",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: appsv1.DeploymentConfigSpec{
			Replicas: replicas,
		},
	}
}

func rc(name, namespace, dcName string, replicas int32, labels, annotations map[string]string) *corev1.ReplicationController {
	isController := true
	rep := &replicas
	return &corev1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind: "ReplicationController",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind:               "DeploymentConfig",
					Name:               dcName,
					Controller:         &isController,
					BlockOwnerDeletion: &isController,
				},
			},
		},
		Spec: corev1.ReplicationControllerSpec{
			Replicas: rep,
		},
	}
}

// TODO: Add OwnerRefs for rs,ss,dep?
func rs(name, namespace string, replicas int32, labels, annotations map[string]string) *kappsv1.ReplicaSet {
	rep := &replicas
	return &kappsv1.ReplicaSet{
		TypeMeta: metav1.TypeMeta{
			Kind: "ReplicaSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: kappsv1.ReplicaSetSpec{
			Replicas: rep,
		},
	}
}

// TODO: Add OwnerRefs for rs,ss,dep?
func ss(name, namespace string, replicas int32, labels, annotations map[string]string) *kappsv1.StatefulSet {
	rep := &replicas
	return &kappsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind: "StatefulSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: kappsv1.StatefulSetSpec{
			Replicas: rep,
		},
	}
}

// TODO: Add OwnerRefs for rs,ss,dep?
func dep(name, namespace string, replicas int32, labels, annotations map[string]string) *kappsv1.Deployment {
	rep := &replicas
	return &kappsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			Kind: "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: kappsv1.DeploymentSpec{
			Replicas: rep,
		},
	}
}

func svc(name, namespace string, selectors map[string]string) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind: "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: selectors,
		},
	}
}

func activeNamespace(name string, labels, annotations map[string]string) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			Kind: "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: annotations,
		},
		Status: corev1.NamespaceStatus{
			Phase: corev1.NamespaceActive,
		},
	}
}

func terminatingNamespace(name string, labels, annotations map[string]string) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			Kind: "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: annotations,
		},
		Status: corev1.NamespaceStatus{
			Phase: corev1.NamespaceTerminating,
		},
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
