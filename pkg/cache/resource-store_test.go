package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	fakeoclientset "github.com/openshift/client-go/apps/clientset/versioned/fake"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakekclientset "k8s.io/client-go/kubernetes/fake"

	ktesting "k8s.io/client-go/testing"
	kcache "k8s.io/client-go/tools/cache"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

func setupClients(t *testing.T) (*fakekclientset.Clientset, *fakeoclientset.Clientset, time.Time) {
	maxDeployDurSeconds := int64(8)
	deltimenow := time.Now()
	deltimeunver := metav1.NewTime(deltimenow)
	deltime := &deltimeunver
	fakeOClient := fakeoclientset.NewSimpleClientset()
	fakeClient := &fakekclientset.Clientset{}

	fakeClient.AddReactor("list", "services", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
		obj := &corev1.ServiceList{
			Items: []corev1.Service{
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Service",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:            "somesvc1",
						Namespace:       "somens1",
						UID:             "1234",
						ResourceVersion: "1",
					},
					Spec: corev1.ServiceSpec{
						Selector: map[string]string{"foo": "bar"},
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Service",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:            "somesvc2",
						Namespace:       "somens2",
						UID:             "5678",
						ResourceVersion: "7",
					},
					Spec: corev1.ServiceSpec{
						Selector: map[string]string{"baz": "quux"},
					},
				},
			},
		}

		return true, obj, nil
	})

	fakeClient.AddReactor("list", "namespaces", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
		obj := &corev1.NamespaceList{
			Items: []corev1.Namespace{
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "somens1",
					},
					Status: corev1.NamespaceStatus{
						Phase: corev1.NamespaceActive,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "somens2",
					},
					Status: corev1.NamespaceStatus{
						Phase: corev1.NamespaceTerminating,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "somens3",
					},
					Status: corev1.NamespaceStatus{
						Phase: corev1.NamespaceActive,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "somens4",
					},
					Status: corev1.NamespaceStatus{
						Phase: corev1.NamespaceActive,
					},
				},
			},
		}

		return true, obj, nil
	})

	fakeClient.AddReactor("list", "replicationcontrollers", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
		obj := &corev1.ReplicationControllerList{
			Items: []corev1.ReplicationController{
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "ReplicationController",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somerc1",
						Namespace:         "somens1",
						UID:               "2345",
						ResourceVersion:   "1",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"somerclabel": "rcsomething"},
						Annotations:       map[string]string{OpenShiftDCName: "somedc"},
					},
					Spec: corev1.ReplicationControllerSpec{
						Selector: map[string]string{"somercselector": "rcblah"},
					},
					Status: corev1.ReplicationControllerStatus{
						Replicas: 1,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "ReplicationController",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somerc2",
						Namespace:         "somens2",
						UID:               "3456",
						ResourceVersion:   "1",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"anotherrclabel": "rcanother"},
						Annotations:       map[string]string{"openshift.io/deployment-config.name": "anotherdc"},
					},
					Spec: corev1.ReplicationControllerSpec{
						Selector: map[string]string{"anotherrcselector": "rcblahblah"},
					},
					Status: corev1.ReplicationControllerStatus{
						Replicas: 1,
					},
				},
			},
		}

		return true, obj, nil
	})

	fakeClient.AddReactor("list", "replicasets", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
		obj := &v1beta1.ReplicaSetList{
			Items: []v1beta1.ReplicaSet{
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "ReplicaSet",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somers1",
						Namespace:         "somens3",
						UID:               "3345",
						ResourceVersion:   "1",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"somerslabel": "rssomething"},
					},
					Spec: v1beta1.ReplicaSetSpec{
						Selector: &metav1.LabelSelector{MatchLabels: (map[string]string{"somersselector": "rsblah"})},
					},
					Status: v1beta1.ReplicaSetStatus{
						Replicas: 1,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "ReplicaSet",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somers2",
						Namespace:         "somens4",
						UID:               "4456",
						ResourceVersion:   "1",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"anotherrslabel": "rsanother"},
					},
					Spec: v1beta1.ReplicaSetSpec{
						Selector: &metav1.LabelSelector{MatchLabels: (map[string]string{"anotherrsselector": "rsblahblah"})},
					},
					Status: v1beta1.ReplicaSetStatus{
						Replicas: 1,
					},
				},
			},
		}

		return true, obj, nil
	})

	fakeClient.AddReactor("list", "pods", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
		obj := &corev1.PodList{
			Items: []corev1.Pod{
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somepod1",
						Namespace:         "somens1",
						UID:               "1122",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"somepodlabel": "podsomething"},
						Annotations:       map[string]string{OpenShiftDCName: "poddc"},
					},
					Spec: corev1.PodSpec{
						RestartPolicy:         corev1.RestartPolicyOnFailure,
						ActiveDeadlineSeconds: &maxDeployDurSeconds,
						Containers: []corev1.Container{
							{
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										"memory": resource.MustParse("0"),
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somepod2",
						Namespace:         "somens2",
						UID:               "2211",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"anotherpodlabel": "podsomethingelse"},
						Annotations:       map[string]string{OpenShiftDCName: "anotherpoddc"},
					},
					Spec: corev1.PodSpec{
						RestartPolicy:         corev1.RestartPolicyAlways,
						ActiveDeadlineSeconds: nil,
						Containers: []corev1.Container{
							{
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										"memory": resource.MustParse("0"),
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodUnknown,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somepod3",
						Namespace:         "somens3",
						UID:               "2211",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"yetanotherpodlabel": "podsomethingelseagain"},
						Annotations:       map[string]string{},
					},
					Spec: corev1.PodSpec{
						RestartPolicy:         corev1.RestartPolicyAlways,
						ActiveDeadlineSeconds: nil,
						Containers: []corev1.Container{
							{
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										"memory": resource.MustParse("0"),
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somepod4",
						Namespace:         "somens4",
						UID:               "2211",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"yetanotherpodlabelagain": "podsomethingelseagainandagain"},
						Annotations:       map[string]string{},
					},
					Spec: corev1.PodSpec{
						RestartPolicy:         corev1.RestartPolicyAlways,
						ActiveDeadlineSeconds: nil,
						Containers: []corev1.Container{
							{
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										"memory": resource.MustParse("0"),
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodUnknown,
					},
				},
			},
		}

		return true, obj, nil
	})

	return fakeClient, fakeOClient, deltimenow
}

func TestCacheIsInitiallyPopulated(t *testing.T) {
	c := make(chan struct{})
	fakeClient, fakeOClient, _ := setupClients(t)
	var exclude map[string]bool
	store := NewResourceStore(exclude, fakeOClient, fakeClient)

	svcLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return fakeClient.CoreV1().Services(corev1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return fakeClient.CoreV1().Services(corev1.NamespaceAll).Watch(options)
		},
	}

	svcr := kcache.NewReflector(svcLW, &corev1.Service{}, store, 0)
	go svcr.Run(c)
	time.Sleep(1 * time.Second)

	svcs, err := store.ByIndex("byNamespaceAndKind", "somens1/"+ServiceKind)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if assert.Len(t, svcs, 1, "expected to have one service in namespace somens1") {
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resource := &ResourceObject{
			UID:             "1234",
			Name:            "somesvc1",
			Namespace:       "somens1",
			Kind:            ServiceKind,
			ResourceVersion: "1",
			Selectors:       selector,
		}
		assert.Equal(t, resource, svcs[0], "expected the service somesvc1 to be converted to a resource object properly")
	}
}

func TestReplaceWithMultipleDoesNotConflict(t *testing.T) {
	c := make(chan struct{})
	defer close(c)
	fakeClient, fakeOClient, deltime := setupClients(t)
	store := NewResourceStore(map[string]bool{}, fakeOClient, fakeClient)

	svcLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return fakeClient.CoreV1().Services(corev1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return fakeClient.CoreV1().Services(corev1.NamespaceAll).Watch(options)
		},
	}
	rcLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return fakeClient.CoreV1().ReplicationControllers(corev1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return fakeClient.CoreV1().ReplicationControllers(corev1.NamespaceAll).Watch(options)
		},
	}
	rsLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return fakeClient.ExtensionsV1beta1().ReplicaSets(corev1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return fakeClient.ExtensionsV1beta1().ReplicaSets(corev1.NamespaceAll).Watch(options)
		},
	}
	podLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return fakeClient.CoreV1().Pods(corev1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return fakeClient.CoreV1().Pods(corev1.NamespaceAll).Watch(options)
		},
	}
	nsLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return fakeClient.CoreV1().Namespaces().List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return fakeClient.CoreV1().Namespaces().Watch(options)
		},
	}

	svcr := kcache.NewReflector(svcLW, &corev1.Service{}, store, 0)
	go svcr.Run(c)
	nsr := kcache.NewReflector(nsLW, &corev1.Namespace{}, store, 0)
	go nsr.Run(c)
	rcr := kcache.NewReflector(rcLW, &corev1.ReplicationController{}, store, 0)
	go rcr.Run(c)
	rsr := kcache.NewReflector(rsLW, &v1beta1.ReplicaSet{}, store, 0)
	go rsr.Run(c)
	podr := kcache.NewReflector(podLW, &corev1.Pod{}, store, 0)
	go podr.Run(c)
	time.Sleep(1 * time.Second)

	svcs, err := store.ByIndex("byNamespaceAndKind", "somens1/"+ServiceKind)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if assert.Len(t, svcs, 1, "expected to have one service in namespace somens1") {
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resource := &ResourceObject{
			UID:             "1234",
			Name:            "somesvc1",
			Namespace:       "somens1",
			Kind:            ServiceKind,
			ResourceVersion: "1",
			Selectors:       selector,
		}
		assert.Equal(t, resource, svcs[0], "expected the service somesvc1 to be converted to a resource object properly")
	}

	rcs, err := store.ByIndex("byNamespaceAndKind", "somens1/"+RCKind)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if assert.Len(t, rcs, 1, "expected to have one rc in namespace somens1") {
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"somercselector": "rcblah"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resource := &ResourceObject{
			UID:               "2345",
			Name:              "somerc1",
			Namespace:         "somens1",
			Kind:              RCKind,
			ResourceVersion:   "1",
			DeletionTimestamp: deltime,
			Selectors:         selector,
			Labels:            map[string]string{"somerclabel": "rcsomething"},
		}
		assert.NotNil(t, rcs[0].(*ResourceObject).RunningTimes, "expected the rc somerc1 to be converted to a resource object properly, found nil RunningTime")
		assert.Equal(t, resource.UID, rcs[0].(*ResourceObject).UID, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.Name, rcs[0].(*ResourceObject).Name, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.Namespace, rcs[0].(*ResourceObject).Namespace, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.Kind, rcs[0].(*ResourceObject).Kind, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.ResourceVersion, rcs[0].(*ResourceObject).ResourceVersion, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.DeletionTimestamp, rcs[0].(*ResourceObject).DeletionTimestamp, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.Selectors, rcs[0].(*ResourceObject).Selectors, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.Labels, rcs[0].(*ResourceObject).Labels, "expected the rc somerc1 to be converted to a resource object properly")
	}

	rss, err := store.ByIndex("byNamespaceAndKind", "somens3/"+RSKind)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if assert.Len(t, rss, 1, "expected to have one rs in namespace somens3") {
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"somersselector": "rsblah"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resource := &ResourceObject{
			UID:               "3345",
			Name:              "somers1",
			Namespace:         "somens3",
			Kind:              RSKind,
			ResourceVersion:   "1",
			DeletionTimestamp: deltime,
			Selectors:         selector,
			Labels:            map[string]string{"somerslabel": "rssomething"},
		}
		assert.NotNil(t, rss[0].(*ResourceObject).RunningTimes, "expected the rs somers1 to be converted to a resource object properly, found nil RunningTime")
		assert.Equal(t, resource.UID, rss[0].(*ResourceObject).UID, "expected the rs somers1 to be converted to a resource object properly")
		assert.Equal(t, resource.Name, rss[0].(*ResourceObject).Name, "expected the rs somers1 to be converted to a resource object properly")
		assert.Equal(t, resource.Namespace, rss[0].(*ResourceObject).Namespace, "expected the rs somers1 to be converted to a resource object properly")
		assert.Equal(t, resource.Kind, rss[0].(*ResourceObject).Kind, "expected the rs somers1 to be converted to a resource object properly")
		assert.Equal(t, resource.ResourceVersion, rss[0].(*ResourceObject).ResourceVersion, "expected the rs somers1 to be converted to a resource object properly")
		assert.Equal(t, resource.DeletionTimestamp, rss[0].(*ResourceObject).DeletionTimestamp, "expected the rs somers1 to be converted to a resource object properly")
		assert.Equal(t, resource.Selectors, rss[0].(*ResourceObject).Selectors, "expected the rs somers1 to be converted to a resource object properly")
		assert.Equal(t, resource.Labels, rss[0].(*ResourceObject).Labels, "expected the rs somers1 to be converted to a resource object properly")
	}

	pods, err := store.ByIndex("byNamespaceAndKind", "somens1/"+PodKind)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if assert.Len(t, pods, 1, "expected to have one pod in namespace somens1") {
		resource := &ResourceObject{
			UID:               "1122",
			Name:              "somepod1",
			Namespace:         "somens1",
			Kind:              PodKind,
			Terminating:       true,
			DeletionTimestamp: deltime,
			Labels:            map[string]string{"somepodlabel": "podsomething"},
			Annotations:       map[string]string{OpenShiftDCName: "poddc"},
		}
		assert.NotNil(t, pods[0].(*ResourceObject).RunningTimes, "expected the pod somepod1 to be converted to a resource object properly, found nil RunningTime")
		assert.NotNil(t, pods[0].(*ResourceObject).MemoryRequested, "expected the pod somepod1 to be converted to a resource object properly, found nil RunningTime")
		assert.Equal(t, resource.UID, pods[0].(*ResourceObject).UID, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.Name, pods[0].(*ResourceObject).Name, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.Namespace, pods[0].(*ResourceObject).Namespace, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.Kind, pods[0].(*ResourceObject).Kind, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.Annotations, pods[0].(*ResourceObject).Annotations, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.DeletionTimestamp, pods[0].(*ResourceObject).DeletionTimestamp, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.Labels, pods[0].(*ResourceObject).Labels, "expected the pod somepod1 to be converted to a resource object properly")
	}

	rspods, err := store.ByIndex("byNamespaceAndKind", "somens3/"+PodKind)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if assert.Len(t, pods, 1, "expected to have one pod in namespace somens3") {
		resource := &ResourceObject{
			UID:               "2211",
			Name:              "somepod3",
			Namespace:         "somens3",
			Kind:              PodKind,
			Terminating:       true,
			DeletionTimestamp: deltime,
			Labels:            map[string]string{"yetanotherpodlabel": "podsomethingelseagain"},
			Annotations:       map[string]string{},
		}
		assert.NotNil(t, rspods[0].(*ResourceObject).RunningTimes, "expected the pod somepod3 to be converted to a resource object properly, found nil RunningTime")
		assert.NotNil(t, rspods[0].(*ResourceObject).MemoryRequested, "expected the pod somepod3 to be converted to a resource object properly, found nil RunningTime")
		assert.Equal(t, resource.UID, rspods[0].(*ResourceObject).UID, "expected the pod somepod3 to be converted to a resource object properly")
		assert.Equal(t, resource.Name, rspods[0].(*ResourceObject).Name, "expected the pod somepod3 to be converted to a resource object properly")
		assert.Equal(t, resource.Namespace, rspods[0].(*ResourceObject).Namespace, "expected the pod somepod3 to be converted to a resource object properly")
		assert.Equal(t, resource.Kind, rspods[0].(*ResourceObject).Kind, "expected the pod somepod3 to be converted to a resource object properly")
		assert.Equal(t, resource.Annotations, rspods[0].(*ResourceObject).Annotations, "expected the pod somepod3 to be converted to a resource object properly")
		assert.Equal(t, resource.DeletionTimestamp, rspods[0].(*ResourceObject).DeletionTimestamp, "expected the pod somepod3 to be converted to a resource object properly")
		assert.Equal(t, resource.Labels, rspods[0].(*ResourceObject).Labels, "expected the pod somepod3 to be converted to a resource object properly")
	}

	nses, err := store.ByIndex("getProject", "somens1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if assert.Len(t, nses, 1, "expected to have one namespace called somens1") {
		resource := &ResourceObject{
			UID:       "somens1",
			Name:      "somens1",
			Namespace: "somens1",
			Kind:      ProjectKind,
		}
		assert.Equal(t, resource, nses[0], "expected the namespace somens1 to be converted to a resource object properly")
	}
}
