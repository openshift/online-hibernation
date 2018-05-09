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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kappsv1 "k8s.io/api/apps/v1"
	fakekclientset "k8s.io/client-go/kubernetes/fake"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakeidlersclient "github.com/openshift/service-idler/pkg/client/clientset/versioned/typed/idling/v1alpha2/fake"
	ktesting "k8s.io/client-go/testing"
	kcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
)

func makeServiceLister(services []*corev1.Service) listerscorev1.ServiceLister {
	c := kcache.NewIndexer(kcache.MetaNamespaceKeyFunc, kcache.Indexers{kcache.NamespaceIndex: kcache.MetaNamespaceIndexFunc})
	for _, svc := range services {
		if err := c.Add(svc); err != nil {
			fmt.Errorf("%s", err)
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
			fmt.Errorf("%s", err)
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
			fmt.Errorf("%s", err)
			return nil
		}
	}
	lister := listerscorev1.NewPodLister(c)
	return lister
}

func init() {
	log.SetOutput(os.Stdout)
}

func NewFakeResourceStore(pods []*corev1.Pod, projects []*corev1.Namespace, services []*corev1.Service) *cache.ResourceStore {
	resourceStore := &cache.ResourceStore {
		KubeClient: fakekclientset.NewSimpleClientset(),
		PodList: makePodLister(pods),
		ProjectList: makeNamespaceLister(projects),
		ServiceList: makeServiceLister(services),
		ClientPool: &fakedynamic.FakeClientPool{},
		IdlersClient: &fakeidlersclient.FakeIdlingV1alpha2{},
	}
	return resourceStore
}

func containerListForMemory(memory string)[]corev1.Container {
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
		projects			   []*corev1.Namespace
		services               []*corev1.Service
		replicationControllers []*corev1.ReplicationController
		statefulSets		   []*kappsv1.StatefulSet
		replicaSets			   []*kappsv1.ReplicaSet
		deployments			   []*kappsv1.Deployment
		deploymentConfigs      []*appsv1.DeploymentConfig
		expectedQueueLen       int
		expectedQueueKeys      []string
	}{
		"Single item added to queue": {
			idleDryRun: false,
			netmap:     map[string]float64{"somens1": 1000},
			//func deletedPod(name, namespace string, labels map[string]string, activeDeadlineSeconds *int64, memory string, startTime metav1.Time, deltime metav1.Time) *corev1.Pod {
			//func pod(name, namespace string, labels map[string]string, activeDeadlineSeconds *int64, memory string, startTime metav1.Time, containerListForMemory []corev1.Container) *corev1.Pod {
			pods: []*corev1.Pod{
				pod("somepod1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "500Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
			},
			//func svc(name, namespace string, selector map[string]string) *corev1.Service {
			services: []*corev1.Service{
				svc("somesvc1", "somens1", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
			},
			//func rc(name, namespace, dcName string, replicas *int32, labels, annotations map[string]string) *corev1.ReplicationController {
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "somens1", "apoddc", 1, map[string]string{},map[string]string{}),
			},
			// NOTE: int32 not *int32
			//func dc(name, namespace string, replicas int32, labels, annotations map[string]string) *appsv1.DeploymentConfig {
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "somens1", 1, map[string]string{}, map[string]string{}),
			},
			//func terminatingNamespace(name, namespace string, labels, annotations map[string]string) *corev1.Namespace {
			projects: []*corev1.Namespace{
				activeNamespace("somens1", "somens1", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
				terminatingNamespace("someDeletedNS", "someDeletedNS", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			//func ss(name, namespace string, replicas *int32, labels, annotations map[string]string) *kappsv1.StatefulSet {
			expectedQueueLen:  1,
			expectedQueueKeys: []string{"somens1"},
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
		fakeOClient := fakeoclientset.NewSimpleClientset()
		fakeClient := fakekclientset.NewSimpleClientset()

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

		resources := NewFakeResourceStore(test.pods, test.projects, test.services)

		podList, _ := resources.PodList.Pods("somens1").List(labels.Everything())
		assert.Equal(t, len(podList), 1, "expected a pod")
		svcList, _ := resources.ServiceList.Services("somens1").List(labels.Everything())
		assert.Equal(t, len(svcList), 1, "expected a service in resourceStore")
		podsInNS, _ := resources.PodList.Pods("somens1").List(labels.Everything())
		assert.Equal(t, len(podsInNS), 1, "expected a pod in resourceStore")
		nsInStore, _ := resources.ProjectList.List(labels.Everything())
		assert.Equal(t, len(nsInStore), 2, "expected a ns in resourceStore")
		idler := NewAutoIdler(config, resources)
		idler.sync(test.netmap)
		assert.Equal(t, idler.queue.Len(), test.expectedQueueLen, "expected items did not match actual items in workqueue")
		nsList := examineQueue(idler.queue)
		assert.Equal(t, nsList, test.expectedQueueKeys, "unexpected queue contents")
		idler.queue.ShutDown()

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
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:      corev1.RestartPolicyAlways,
			ActiveDeadlineSeconds: activeDeadlineSeconds,
			Containers: containerList,
		},
		Status: corev1.PodStatus{
			StartTime:     &startTime,
			Phase: phase,
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
			Name:      name,
			Namespace: namespace,
			Labels: labels,
			DeletionTimestamp: &deltime,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:      corev1.RestartPolicyAlways,
			ActiveDeadlineSeconds: activeDeadlineSeconds,
			Containers: containerList,
		},
		Status: corev1.PodStatus{
			StartTime:     &startTime,
			Phase: phase,
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
			Name:      name,
			Namespace: namespace,
			Labels: labels,
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
			Name:      name,
			Namespace: namespace,
			Labels: labels,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind: "DeploymentConfig",
					Name: dcName,
					Controller: &isController,
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
			Name:      name,
			Namespace: namespace,
			Labels: labels,
			Annotations: annotations,
		},
		Spec: kappsv1.ReplicaSetSpec{
			Replicas: rep,
		},
	}
}

// TODO: Add OwnerRefs for rs,ss,dep?
func ss(name, namespace string, replicas int32, labels, annotations map[string]string) *kappsv1.StatefulSet {
	rep :=&replicas
	return &kappsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind: "StatefulSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: labels,
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
			Name:      name,
			Namespace: namespace,
			Labels: labels,
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

func activeNamespace(name, namespace string, labels, annotations map[string]string) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			Kind: "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: labels,
			Annotations: annotations,
		},
		Status: corev1.NamespaceStatus{
			Phase: corev1.NamespaceActive,
		},
	}
}

func terminatingNamespace(name, namespace string, labels, annotations map[string]string) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			Kind: "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: labels,
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
