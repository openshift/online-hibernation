package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/api/unversioned"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	ktestclient "k8s.io/kubernetes/pkg/client/unversioned/testclient"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/watch"

	deployapi "github.com/openshift/origin/pkg/deploy/api"
)

func setupClients(t *testing.T) (*ktestclient.Fake, time.Time) {
	maxDeployDurSeconds := deployapi.MaxDeploymentDurationSeconds
	deltimenow := time.Now()
	deltimeunver := unversioned.NewTime(deltimenow)
	deltime := &deltimeunver
	fakeClient := &ktestclient.Fake{}
	fakeClient.AddReactor("list", "services", func(action ktestclient.Action) (handled bool, resp runtime.Object, err error) {
		obj := &kapi.ServiceList{
			Items: []kapi.Service{
				{
					TypeMeta: unversioned.TypeMeta{
						Kind:       "Service",
						APIVersion: "v1",
					},
					ObjectMeta: kapi.ObjectMeta{
						Name:            "somesvc1",
						Namespace:       "somens1",
						UID:             "1234",
						ResourceVersion: "1",
					},
					Spec: kapi.ServiceSpec{
						Selector: map[string]string{"foo": "bar"},
					},
				},
				{
					TypeMeta: unversioned.TypeMeta{
						Kind:       "Service",
						APIVersion: "v1",
					},
					ObjectMeta: kapi.ObjectMeta{
						Name:            "somesvc2",
						Namespace:       "somens2",
						UID:             "5678",
						ResourceVersion: "7",
					},
					Spec: kapi.ServiceSpec{
						Selector: map[string]string{"baz": "quux"},
					},
				},
			},
		}

		return true, obj, nil
	})

	fakeClient.AddReactor("list", "namespaces", func(action ktestclient.Action) (handled bool, resp runtime.Object, err error) {
		obj := &kapi.NamespaceList{
			Items: []kapi.Namespace{
				{
					TypeMeta: unversioned.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: kapi.ObjectMeta{
						Name: "somens1",
					},
					Status: kapi.NamespaceStatus{
						Phase: kapi.NamespaceActive,
					},
				},
				{
					TypeMeta: unversioned.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: kapi.ObjectMeta{
						Name: "somens2",
					},
					Status: kapi.NamespaceStatus{
						Phase: kapi.NamespaceTerminating,
					},
				},
			},
		}

		return true, obj, nil
	})

	fakeClient.AddReactor("list", "replicationcontrollers", func(action ktestclient.Action) (handled bool, resp runtime.Object, err error) {
		obj := &kapi.ReplicationControllerList{
			Items: []kapi.ReplicationController{
				{
					TypeMeta: unversioned.TypeMeta{
						Kind:       "ReplicationController",
						APIVersion: "v1",
					},
					ObjectMeta: kapi.ObjectMeta{
						Name:              "somerc1",
						Namespace:         "somens1",
						UID:               "2345",
						ResourceVersion:   "1",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"somerclabel": "rcsomething"},
						Annotations:       map[string]string{OpenShiftDCName: "somedc"},
					},
					Spec: kapi.ReplicationControllerSpec{
						Selector: map[string]string{"somercselector": "rcblah"},
					},
					Status: kapi.ReplicationControllerStatus{
						Replicas: 1,
					},
				},
				{
					TypeMeta: unversioned.TypeMeta{
						Kind:       "ReplicationController",
						APIVersion: "v1",
					},
					ObjectMeta: kapi.ObjectMeta{
						Name:              "somerc2",
						Namespace:         "somens2",
						UID:               "3456",
						ResourceVersion:   "1",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"anotherrclabel": "rcanother"},
						Annotations:       map[string]string{"openshift.io/deployment-config.name": "anotherdc"},
					},
					Spec: kapi.ReplicationControllerSpec{
						Selector: map[string]string{"anotherrcselector": "rcblahblah"},
					},
					Status: kapi.ReplicationControllerStatus{
						Replicas: 1,
					},
				},
			},
		}

		return true, obj, nil
	})

	fakeClient.AddReactor("list", "pods", func(action ktestclient.Action) (handled bool, resp runtime.Object, err error) {
		obj := &kapi.PodList{
			Items: []kapi.Pod{
				{
					TypeMeta: unversioned.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: kapi.ObjectMeta{
						Name:              "somepod1",
						Namespace:         "somens1",
						UID:               "1122",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"somepodlabel": "podsomething"},
						Annotations:       map[string]string{OpenShiftDCName: "poddc"},
					},
					Spec: kapi.PodSpec{
						RestartPolicy:         kapi.RestartPolicyOnFailure,
						ActiveDeadlineSeconds: &maxDeployDurSeconds,
						Containers: []kapi.Container{
							{
								Resources: kapi.ResourceRequirements{
									Requests: kapi.ResourceList{
										"memory": resource.MustParse("0"),
									},
								},
							},
						},
					},
					Status: kapi.PodStatus{
						Phase: kapi.PodRunning,
					},
				},
				{
					TypeMeta: unversioned.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: kapi.ObjectMeta{
						Name:              "somepod2",
						Namespace:         "somens2",
						UID:               "2211",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"anotherpodlabel": "podsomethingelse"},
						Annotations:       map[string]string{OpenShiftDCName: "anotherpoddc"},
					},
					Spec: kapi.PodSpec{
						RestartPolicy:         kapi.RestartPolicyAlways,
						ActiveDeadlineSeconds: nil,
						Containers: []kapi.Container{
							{
								Resources: kapi.ResourceRequirements{
									Requests: kapi.ResourceList{
										"memory": resource.MustParse("0"),
									},
								},
							},
						},
					},
					Status: kapi.PodStatus{
						Phase: kapi.PodUnknown,
					},
				},
			},
		}

		return true, obj, nil
	})

	return fakeClient, deltimenow
}

func TestCacheIsInitiallyPopulated(t *testing.T) {
	store := NewResourceStore(map[string]bool{})
	fakeClient, _ := setupClients(t)

	svcLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return fakeClient.Services(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return fakeClient.Services(kapi.NamespaceAll).Watch(options)
		},
	}

	stopCh := make(chan struct{})
	defer close(stopCh)
	kcache.NewReflector(svcLW, &kapi.Service{}, store, 0).RunUntil(stopCh)
	time.Sleep(1 * time.Second)

	svcs, err := store.ByIndex("byNamespaceAndKind", "somens1/"+ServiceKind)
	if err != nil {
		t.Fatalf("unexpected error: %v")
	}

	if assert.Len(t, svcs, 1, "expected to have one service in namespace somens1") {
		resource := &ResourceObject{
			UID:             "1234",
			Name:            "somesvc1",
			Namespace:       "somens1",
			Kind:            ServiceKind,
			ResourceVersion: "1",
			Selectors:       map[string]string{"foo": "bar"},
		}
		assert.Equal(t, resource, svcs[0], "expected the service somesvc1 to be converted to a resource object properly")
	}
}

func TestReplaceWithMultipleDoesNotConflict(t *testing.T) {
	store := NewResourceStore(map[string]bool{})
	fakeClient, deltime := setupClients(t)

	svcLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return fakeClient.Services(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return fakeClient.Services(kapi.NamespaceAll).Watch(options)
		},
	}
	rcLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return fakeClient.ReplicationControllers(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return fakeClient.ReplicationControllers(kapi.NamespaceAll).Watch(options)
		},
	}
	podLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return fakeClient.Pods(kapi.NamespaceAll).List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return fakeClient.Pods(kapi.NamespaceAll).Watch(options)
		},
	}
	nsLW := &kcache.ListWatch{
		ListFunc: func(options kapi.ListOptions) (runtime.Object, error) {
			return fakeClient.Namespaces().List(options)
		},
		WatchFunc: func(options kapi.ListOptions) (watch.Interface, error) {
			return fakeClient.Namespaces().Watch(options)
		},
	}

	stopCh := make(chan struct{})
	defer close(stopCh)
	kcache.NewReflector(svcLW, &kapi.Service{}, store, 0).RunUntil(stopCh)
	kcache.NewReflector(nsLW, &kapi.Namespace{}, store, 0).RunUntil(stopCh)
	kcache.NewReflector(rcLW, &kapi.ReplicationController{}, store, 0).RunUntil(stopCh)
	kcache.NewReflector(podLW, &kapi.Pod{}, store, 0).RunUntil(stopCh)
	time.Sleep(1 * time.Second)

	svcs, err := store.ByIndex("byNamespaceAndKind", "somens1/"+ServiceKind)
	if err != nil {
		t.Fatalf("unexpected error: %v")
	}

	if assert.Len(t, svcs, 1, "expected to have one service in namespace somens1") {
		resource := &ResourceObject{
			UID:             "1234",
			Name:            "somesvc1",
			Namespace:       "somens1",
			Kind:            ServiceKind,
			ResourceVersion: "1",
			Selectors:       map[string]string{"foo": "bar"},
		}
		assert.Equal(t, resource, svcs[0], "expected the service somesvc1 to be converted to a resource object properly")
	}

	rcs, err := store.ByIndex("byNamespaceAndKind", "somens1/"+RCKind)
	if err != nil {
		t.Fatalf("unexpected error: %v")
	}

	if assert.Len(t, rcs, 1, "expected to have one rc in namespace somens1") {
		resource := &ResourceObject{
			UID:               "2345",
			Name:              "somerc1",
			Namespace:         "somens1",
			Kind:              RCKind,
			ResourceVersion:   "1",
			DeploymentConfig:  "somedc",
			DeletionTimestamp: deltime,
			Selectors:         map[string]string{"somercselector": "rcblah"},
			Labels:            map[string]string{"somerclabel": "rcsomething"},
		}
		assert.NotNil(t, rcs[0].(*ResourceObject).RunningTimes, "expected the rc somerc1 to be converted to a resource object properly, found nil RunningTime")
		assert.Equal(t, resource.UID, rcs[0].(*ResourceObject).UID, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.Name, rcs[0].(*ResourceObject).Name, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.Namespace, rcs[0].(*ResourceObject).Namespace, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.Kind, rcs[0].(*ResourceObject).Kind, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.ResourceVersion, rcs[0].(*ResourceObject).ResourceVersion, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.DeploymentConfig, rcs[0].(*ResourceObject).DeploymentConfig, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.DeletionTimestamp, rcs[0].(*ResourceObject).DeletionTimestamp, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.Selectors, rcs[0].(*ResourceObject).Selectors, "expected the rc somerc1 to be converted to a resource object properly")
		assert.Equal(t, resource.Labels, rcs[0].(*ResourceObject).Labels, "expected the rc somerc1 to be converted to a resource object properly")
	}

	pods, err := store.ByIndex("byNamespaceAndKind", "somens1/"+PodKind)
	if err != nil {
		t.Fatalf("unexpected error: %v")
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

	nses, err := store.ByIndex("getProject", "somens1")
	if err != nil {
		t.Fatalf("unexpected error: %v")
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
