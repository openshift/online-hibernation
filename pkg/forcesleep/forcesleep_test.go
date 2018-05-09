package forcesleep

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

	svcidler "github.com/openshift/service-idler/pkg/apis/idling/v1alpha2"
	fakeidlersclient "github.com/openshift/service-idler/pkg/client/clientset/versioned/fake"
	iclient "github.com/openshift/service-idler/pkg/client/clientset/versioned/typed/idling/v1alpha2"
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

func init() {
	log.SetOutput(os.Stdout)
}

func TestSyncProject(t *testing.T) {
	rightNow := time.Now()
	twoHoursAgo := rightNow.Add(-2 * time.Hour)
	twoHoursAgoString := twoHoursAgo.Format(time.RFC3339)
	nowString := rightNow.Format(time.RFC3339)
	tests := map[string]struct {
		dryRun                 bool
		quota                  string
		period                 string
		projectSleepPeriod     string
		termQuota              string
		nonTermQuota           string
		projects               []*corev1.Namespace
		deploymentConfigs      []*appsv1.DeploymentConfig
		services               []*corev1.Service
		pods                   []*corev1.Pod
		replicationControllers []*corev1.ReplicationController
		isPutAsleep            bool
		isWokenUp              bool
		resources              *cache.ResourceStore
	}{
		"Apply force-sleep to project over quota-hours": {
			isPutAsleep:        true,
			isWokenUp:          false,
			quota:              "1h",
			period:             "12h",
			projectSleepPeriod: "8h",
			termQuota:          "1G",
			nonTermQuota:       "1G",
			dryRun:             false,
			projects: []*corev1.Namespace{
				activeNamespace("test", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			pods: []*corev1.Pod{
				pod("somepod1", "test", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "2Gi", metav1.NewTime(metav1.Now().Add(-16*time.Hour)), corev1.PodRunning),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "test", "apoddc", 1, map[string]string{}, map[string]string{}),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "test", 1, map[string]string{}, map[string]string{}),
			},
			services: []*corev1.Service{
				svc("somesvc1", "test", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
			},
		},

		"Wake project that has been asleep for over sleep-period": {
			isPutAsleep:        false,
			isWokenUp:          true,
			quota:              "16h",
			period:             "24h",
			projectSleepPeriod: "1h",
			termQuota:          "1G",
			nonTermQuota:       "1G",
			dryRun:             false,
			projects: []*corev1.Namespace{
				activeNamespace("test", map[string]string{cache.HibernationLabel: "true"}, map[string]string{cache.ProjectLastSleepTime: twoHoursAgoString}),
			},
			pods: []*corev1.Pod{
				pod("somepod1", "test", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "1Gi", metav1.NewTime(metav1.Now().Add(-16*time.Hour)), corev1.PodRunning),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc1", "test", "apoddc", 1, map[string]string{}, map[string]string{}),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "test", 1, map[string]string{}, map[string]string{}),
			},
			services: []*corev1.Service{
				svc("somesvc1", "test", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
			},
		},

		"Do not apply force-sleep if in dryRun mode": {
			isPutAsleep:        false,
			isWokenUp:          false,
			quota:              "16h",
			period:             "24h",
			projectSleepPeriod: "8h",
			termQuota:          "1G",
			nonTermQuota:       "1G",
			dryRun:             true,
			projects: []*corev1.Namespace{
				activeNamespace("test", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			pods: []*corev1.Pod{
				pod("somepod2", "test", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "1Gi", metav1.NewTime(metav1.Now().Add(-16*time.Hour)), corev1.PodRunning),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc2", "test", "apoddc", 1, map[string]string{}, map[string]string{}),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "test", 1, map[string]string{}, map[string]string{}),
			},
			services: []*corev1.Service{
				svc("somesvc2", "test", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
			},
		},

		"Do not apply force-sleep if not over quota": {
			isPutAsleep:        false,
			isWokenUp:          false,
			quota:              "23h",
			period:             "24h",
			projectSleepPeriod: "8h",
			termQuota:          "1G",
			nonTermQuota:       "1G",
			dryRun:             false,
			projects: []*corev1.Namespace{
				activeNamespace("test", map[string]string{cache.HibernationLabel: "true"}, map[string]string{}),
			},
			pods: []*corev1.Pod{
				pod("somepod2", "test", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}, nil, "100Mi", metav1.NewTime(metav1.Now().Add(-1*time.Hour)), corev1.PodRunning),
			},
			replicationControllers: []*corev1.ReplicationController{
				rc("somerc2", "test", "apoddc", 1, map[string]string{}, map[string]string{}),
			},
			deploymentConfigs: []*appsv1.DeploymentConfig{
				dc("apoddc", "test", 1, map[string]string{}, map[string]string{}),
			},
			services: []*corev1.Service{
				svc("somesvc2", "test", map[string]string{"app": "anapp", "deploymentconfig": "apoddc"}),
			},
		},
	}

	for name, test := range tests {
		t.Logf("Testing: %s", name)

		quota, err := time.ParseDuration(test.quota)
		if err != nil {
			t.Errorf("Error: %s", err)
		}
		period, err := time.ParseDuration(test.period)
		if err != nil {
			t.Errorf("Error: %s", err)
		}
		projectSleepPeriod, err := time.ParseDuration(test.projectSleepPeriod)
		if err != nil {
			t.Errorf("Error: %s", err)
		}

		quotaIsDeleted := false
		quotaIsCreated := false
		idlerIsUpdated := false
		idlerIsCreated := false
		projectIsUpdated := false

		var updatedProjects []*corev1.Namespace
		var updatedIdler *svcidler.Idler
		var gotIdlerName string
		var updatedProject *corev1.Namespace

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

		config := &SleeperConfig{
			Quota:              quota,
			Period:             period,
			ProjectSleepPeriod: projectSleepPeriod,
			TermQuota:          resource.MustParse(test.termQuota),
			NonTermQuota:       resource.MustParse(test.nonTermQuota),
			DryRun:             test.dryRun,
		}

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

		fakeClient.AddReactor("get", "namespaces", func(action ktesting.Action) (bool, runtime.Object, error) {
			gotProjectName := action.(ktesting.GetAction).GetName()
			clearedProjectLastSleepAnnotation := ""
			updatedProjectLastSleepAnnotation := nowString
			var gottenProject *corev1.Namespace
			if test.isPutAsleep {
				gottenProject = activeNamespace(gotProjectName, map[string]string{cache.HibernationLabel: "true"}, map[string]string{cache.ProjectLastSleepTime: updatedProjectLastSleepAnnotation})
			}
			if test.isWokenUp && len(updatedProjects) == 0 {
				gottenProject = activeNamespace(gotProjectName, map[string]string{cache.HibernationLabel: "true"}, map[string]string{cache.ProjectLastSleepTime: twoHoursAgoString})
			}
			if test.isWokenUp && len(updatedProjects) != 0 {
				gottenProject = activeNamespace(gotProjectName, map[string]string{cache.HibernationLabel: "true"}, map[string]string{cache.ProjectLastSleepTime: clearedProjectLastSleepAnnotation})
			}
			return true, gottenProject, nil
		})

		fakeClient.AddReactor("update", "namespaces", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
			projectIsUpdated = true
			clearedProjectLastSleepAnnotation := ""
			updatedProjectLastSleepAnnotation := nowString
			ns := action.(ktesting.UpdateAction).GetObject().(*corev1.Namespace)
			if test.isPutAsleep {
				updatedProject = activeNamespace(ns.Name, ns.Labels, map[string]string{cache.ProjectLastSleepTime: updatedProjectLastSleepAnnotation})
			}
			if test.isWokenUp && len(updatedProjects) == 0 {
				updatedProject = activeNamespace(ns.Name, ns.Labels, map[string]string{cache.ProjectLastSleepTime: twoHoursAgoString})
			}
			if test.isWokenUp && len(updatedProjects) != 0 {
				updatedProject = activeNamespace(ns.Name, ns.Labels, map[string]string{cache.ProjectLastSleepTime: clearedProjectLastSleepAnnotation})
			}
			updatedProjects = append(updatedProjects, updatedProject)
			return true, updatedProject, nil
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

		fakeClient.AddReactor("get", "resourcequotas", func(action ktesting.Action) (bool, runtime.Object, error) {
			if test.isPutAsleep && len(updatedProjects) != 0 {
				return true, resourceQuota(), nil
			}
			if test.isWokenUp && len(updatedProjects) == 0 {
				return true, resourceQuota(), nil
			}
			return true, nil, nil
		})

		fakeClient.AddReactor("create", "resourcequotas", func(action ktesting.Action) (bool, runtime.Object, error) {
			quotaIsCreated = true
			return true, action.(ktesting.CreateAction).GetObject().(*corev1.ResourceQuota), nil
		})

		fakeClient.AddReactor("delete", "resourcequotas", func(action ktesting.Action) (bool, runtime.Object, error) {
			quotaIsDeleted = true
			return true, nil, nil
		})

		fakeIdlersClient.AddReactor("get", "idlers", func(action ktesting.Action) (bool, runtime.Object, error) {
			name := action.(ktesting.GetAction).GetName()
			gotIdlerName = name
			if test.isPutAsleep {
				return true, nil, nil
			}
			if test.isWokenUp {
				return true, idler(name), nil
			}
			return true, nil, nil
		})

		fakeIdlersClient.AddReactor("create", "idlers", func(action ktesting.Action) (bool, runtime.Object, error) {
			idlerIsCreated = true
			return true, action.(ktesting.CreateAction).GetObject().(*svcidler.Idler), nil
		})

		fakeIdlersClient.AddReactor("update", "idlers", func(action ktesting.Action) (bool, runtime.Object, error) {
			idlerIsUpdated = true
			updatedIdler = action.(ktesting.UpdateAction).GetObject().(*svcidler.Idler)
			return true, updatedIdler, nil
		})

		sleeper := NewSleeper(config, resources)
		if test.isPutAsleep {
			sleeper.syncProject("test")
			assert.Equal(t, true, quotaIsCreated, "expected a resource quota in project")
			assert.Equal(t, true, idlerIsCreated, "expected an idler to be created in project")
			assert.Equal(t, true, projectIsUpdated, "expected project to be updated")
			assert.Equal(t, 3, len(updatedProjects), "expected 2 project updates")
			assert.Equal(t, nowString, updatedProjects[2].Annotations[cache.ProjectLastSleepTime], "unexpected updated project")
		}
		if test.isWokenUp {
			sleeper.syncProject("test")
			assert.Equal(t, true, quotaIsDeleted, "expected resource quota to be deleted")
			assert.Equal(t, true, idlerIsUpdated, "expected idler to be updated")
			assert.Equal(t, "hibernation", updatedIdler.Name, "expected updated idler")
			assert.Equal(t, true, projectIsUpdated, "expected project to be updated")
			assert.Equal(t, 2, len(updatedProjects), "expected 2 project updates")
			assert.Equal(t, "", updatedProjects[1].Annotations[cache.ProjectLastSleepTime], "unexpected updated project")
		}
		if !test.isWokenUp && !test.isPutAsleep && !test.dryRun {
			sleeper.syncProject("test")
			assert.Equal(t, false, quotaIsCreated, "did not expected resource quota to be created")
			assert.Equal(t, false, idlerIsCreated, "did not expect idler to be created")
			assert.Equal(t, true, projectIsUpdated, "expected project to be updated")
			assert.Equal(t, 1, len(updatedProjects), "expected 1 project update")
		}
		if !test.isWokenUp && !test.isPutAsleep && test.dryRun {
			sleeper.syncProject("test")
			assert.Equal(t, false, quotaIsCreated, "did not expected resource quota to be created")
			assert.Equal(t, false, idlerIsCreated, "did not expect idler to be created")
			assert.Equal(t, true, projectIsUpdated, "expected project to be updated")
			assert.Equal(t, 1, len(updatedProjects), "expected 1 project update")
		}
	}
}

func resourceQuota() *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name: "force-sleep",
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				"pods": *resource.NewMilliQuantity(0, resource.DecimalSI),
			},
		},
	}
}

func idler(name string) *svcidler.Idler {
	return &svcidler.Idler{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: svcidler.IdlerSpec{
			WantIdle: true,
			TargetScalables: []svcidler.CrossGroupObjectReference{
				svcidler.CrossGroupObjectReference{Group: "agroup", Resource: "aResource", Name: "aResName"},
			},
			TriggerServiceNames: []string{"foo", "bar"},
		},
		Status: svcidler.IdlerStatus{
			UnidledScales:        []svcidler.UnidleInfo{},
			InactiveServiceNames: []string{},
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
