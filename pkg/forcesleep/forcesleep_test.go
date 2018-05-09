package forcesleep

import (
	"log"
	"os"
	"testing"
	"time"

	fakeoclientset "github.com/openshift/client-go/apps/clientset/versioned/fake"
	"github.com/openshift/online-hibernation/pkg/cache"

	appsv1 "github.com/openshift/api/apps/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakekclientset "k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

type action struct {
	Verb     string
	Resource string
	Name     string
}

func init() {
	log.SetOutput(os.Stdout)
}

func TestSyncProject(t *testing.T) {
	tests := map[string]struct {
		DryRun                   bool
		Quota                    string
		Period                   string
		ProjectSleepPeriod       string
		TermQuota                string
		NonTermQuota             string
		Projects                 []*corev1.Namespace
		DeploymentConfigs        []*appsv1.DeploymentConfig
		Pods                     []*corev1.Pod
		ReplicationControllers   []*corev1.ReplicationController
		ResourceQuotas           []*corev1.ResourceQuota
		Resources                []*cache.ResourceObject
		ExpectedOpenshiftActions []action
		ExpectedKubeActions      []action
	}{
		"Apply force-sleep to project over quota-hours": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			DryRun:             false,
			ResourceQuotas:     []*corev1.ResourceQuota{},
			Projects: []*corev1.Namespace{
				project("test"),
			},
			Pods: []*corev1.Pod{
				pod("pod1", "test"),
			},
			ReplicationControllers: []*corev1.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*appsv1.DeploymentConfig{
				dc("dc1", "test"),
			},
			Resources: []*cache.ResourceObject{
				projectResource("test", false, time.Time{}),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
				}),
				podResource("pod1", "pod1", "test", "1",
					resource.MustParse("1G"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
					}),
			},
			ExpectedOpenshiftActions: []action{
				{Verb: "list", Resource: "deploymentconfigs"},
			},
			ExpectedKubeActions: []action{
				{Verb: "get", Resource: "namespaces", Name: "test"},
				{Verb: "create", Resource: "resourcequotas", Name: "force-sleep"},
				{Verb: "list", Resource: "pods"},
				{Verb: "delete", Resource: "pods", Name: "pod1"},
				{Verb: "update", Resource: "namespaces", Name: "test"},
				{Verb: "update", Resource: "replicationcontrollers", Name: "rc1"},
			},
		},

		"Do not apply force-sleep if in dry-run mode and project is supposed to be 'asleep'": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			DryRun:             true,
			ResourceQuotas:     []*corev1.ResourceQuota{},
			Projects: []*corev1.Namespace{
				project("test"),
			},
			Pods: []*corev1.Pod{
				pod("pod1", "test"),
			},
			ReplicationControllers: []*corev1.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*appsv1.DeploymentConfig{
				dc("dc1", "test"),
			},
			// TODO: CONVERT THIS INFO INTO ANNOTATIONS
			Resources: []*cache.ResourceObject{
				projectResource("test", true, time.Now().Add(-4*time.Hour)),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
				}),
				podResource("pod1", "pod1", "test", "1",
					resource.MustParse("1G"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
					}),
			},
			ExpectedOpenshiftActions: []action{},
			ExpectedKubeActions:      []action{},
		},

		"Do not apply force-sleep if in dry-run mode": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			DryRun:             true,
			ResourceQuotas:     []*corev1.ResourceQuota{},
			Projects: []*corev1.Namespace{
				project("test"),
			},
			Pods: []*corev1.Pod{
				pod("pod1", "test"),
			},
			ReplicationControllers: []*corev1.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*appsv1.DeploymentConfig{
				dc("dc1", "test"),
			},
			// TODO: CONVERT THIS INFO INTO ANNOTATIONS
			Resources: []*cache.ResourceObject{
				projectResource("test", false, time.Time{}),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
				}),
				podResource("pod1", "pod1", "test", "1",
					resource.MustParse("1G"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
					}),
			},
			ExpectedOpenshiftActions: []action{},
			ExpectedKubeActions:      []action{},
		},

		"Remove force-sleep from project that has slept for its time": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			DryRun:             false,
			ResourceQuotas: []*corev1.ResourceQuota{
				quota("force-sleep", "test", "1G", "0"),
			},
			Projects: []*corev1.Namespace{
				project("test"),
			},
			Pods: []*corev1.Pod{},
			ReplicationControllers: []*corev1.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*appsv1.DeploymentConfig{
				dc("dc1", "test"),
			},
			// TODO: CONVERT THIS INFO INTO ANNOTATIONS
			Resources: []*cache.ResourceObject{
				projectResource("test", true, time.Now().Add(-8*time.Hour)),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-24*time.Hour),
						time.Now().Add(-8*time.Hour)),
				}),
			},
			ExpectedOpenshiftActions: []action{},
			ExpectedKubeActions: []action{
				{Verb: "get", Resource: "namespaces", Name: "test"},
				{Verb: "delete", Resource: "resourcequotas", Name: "force-sleep"},
			},
		},

		"Do not remove force-sleep from project that hasn't slept for its time": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			DryRun:             false,
			ResourceQuotas: []*corev1.ResourceQuota{
				quota("force-sleep", "test", "1G", "0"),
			},
			Projects: []*corev1.Namespace{
				project("test"),
			},
			Pods: []*corev1.Pod{},
			ReplicationControllers: []*corev1.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*appsv1.DeploymentConfig{
				dc("dc1", "test"),
			},
			// TODO: CONVERT THIS INFO INTO ANNOTATIONS
			Resources: []*cache.ResourceObject{
				projectResource("test", true, time.Now().Add(-4*time.Hour)),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-20*time.Hour),
						time.Now().Add(-4*time.Hour)),
				}),
			},
			ExpectedOpenshiftActions: []action{
				{Verb: "list", Resource: "deploymentconfigs"},
			},
			ExpectedKubeActions: []action{
				{Verb: "get", Resource: "namespaces", Name: "test"},
				{Verb: "update", Resource: "namespaces", Name: "test"},
				{Verb: "update", Resource: "replicationcontrollers"},
				{Verb: "create", Resource: "resourcequotas", Name: "force-sleep"},
			},
		},

		"Apply force-sleep to project with multiple pods": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			DryRun:             false,
			ResourceQuotas:     []*corev1.ResourceQuota{},
			Projects: []*corev1.Namespace{
				project("test"),
			},
			Pods: []*corev1.Pod{
				pod("pod1", "test"),
				pod("pod2", "test"),
			},
			ReplicationControllers: []*corev1.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*appsv1.DeploymentConfig{
				dc("dc1", "test"),
			},
			// TODO: CONVERT THIS INFO INTO ANNOTATIONS
			Resources: []*cache.ResourceObject{
				projectResource("test", false, time.Time{}),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-16*time.Hour),
						time.Time{}),
				}),
				podResource("pod2", "pod2", "test", "1",
					resource.MustParse("500M"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
					}),
				podResource("pod1", "pod1", "test", "1",
					resource.MustParse("500M"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
					}),
			},
			ExpectedOpenshiftActions: []action{
				{Verb: "list", Resource: "deploymentconfigs"},
			},
			ExpectedKubeActions: []action{
				{Verb: "get", Resource: "namespaces", Name: "test"},
				{Verb: "delete", Resource: "pods", Name: "pod1"},
				{Verb: "delete", Resource: "pods", Name: "pod2"},
				{Verb: "create", Resource: "resourcequotas", Name: "force-sleep"},
				{Verb: "list", Resource: "pods"},
				{Verb: "list", Resource: "replicationcontrollers"},
				{Verb: "update", Resource: "replicationcontrollers"},
				{Verb: "update", Resource: "namespaces", Name: "test"},
			},
		},

		"Do not apply force-sleep to a project that doesn't need it": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			DryRun:             false,
			ResourceQuotas:     []*corev1.ResourceQuota{},
			Projects: []*corev1.Namespace{
				project("test"),
			},
			Pods: []*corev1.Pod{
				pod("pod1", "test"),
			},
			ReplicationControllers: []*corev1.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*appsv1.DeploymentConfig{
				dc("dc1", "test"),
			},
			// TODO: CONVERT THIS INFO INTO ANNOTATIONS
			Resources: []*cache.ResourceObject{
				projectResource("test", false, time.Time{}),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-8*time.Hour),
						time.Time{}),
				}),
				podResource("pod1", "pod1", "test", "1",
					resource.MustParse("500M"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-8*time.Hour), time.Time{}),
					}),
			},
			ExpectedOpenshiftActions: []action{},
			ExpectedKubeActions:      []action{},
		},

		"Do not apply force-sleep if memory quota has not been exceeded": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			ResourceQuotas:     []*corev1.ResourceQuota{},
			Pods:               []*corev1.Pod{},
			ReplicationControllers: []*corev1.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*appsv1.DeploymentConfig{
				dc("dc1", "test"),
			},
			// TODO: CONVERT THIS INFO INTO ANNOTATIONS
			Resources: []*cache.ResourceObject{
				projectResource("test", false, time.Time{}),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-16*time.Hour),
						time.Now().Add(-8*time.Hour)),
				}),
			},
			ExpectedOpenshiftActions: []action{},
			ExpectedKubeActions:      []action{},
		},

		"Scale an RC that has been active for quota limit": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			DryRun:             false,
			ResourceQuotas:     []*corev1.ResourceQuota{},
			Projects: []*corev1.Namespace{
				project("test"),
			},
			Pods: []*corev1.Pod{
				pod("pod1", "test"),
			},
			ReplicationControllers: []*corev1.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*appsv1.DeploymentConfig{
				dc("dc1", "test"),
			},
			// TODO: CONVERT THIS INFO INTO ANNOTATIONS
			Resources: []*cache.ResourceObject{
				projectResource("test", false, time.Time{}),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-16*time.Hour),
						time.Time{}),
				}),
				podResource("pod1", "pod1", "test", "1",
					resource.MustParse("1G"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
					}),
			},
			ExpectedOpenshiftActions: []action{
				{Verb: "list", Resource: "deploymentconfigs"},
			},
			ExpectedKubeActions: []action{
				{Verb: "get", Resource: "namespaces", Name: "test"},
				{Verb: "update", Resource: "namespaces", Name: "test"},
				{Verb: "create", Resource: "resourcequotas", Name: "force-sleep"},
				{Verb: "delete", Resource: "pods", Name: "pod1"},
				{Verb: "list", Resource: "pods"},
				{Verb: "list", Resource: "replicationcontrollers"},
				{Verb: "update", Resource: "replicationcontrollers", Name: "rc1"},
			},
		},

		"Scale DC that exceeds running limit (due to multiple active RCs)": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			DryRun:             false,
			ResourceQuotas: []*corev1.ResourceQuota{
				quota("compute-resources", "test", "1G", "2"),
			},
			Projects: []*corev1.Namespace{
				project("test"),
			},
			Pods: []*corev1.Pod{
				pod("pod1", "test"),
				pod("pod2", "test"),
			},
			ReplicationControllers: []*corev1.ReplicationController{
				rc("rc1", "test"),
				rc("rc2", "test"),
			},
			DeploymentConfigs: []*appsv1.DeploymentConfig{
				dc("dc1", "test"),
			},
			// TODO: CONVERT THIS INFO INTO ANNOTATIONS
			Resources: []*cache.ResourceObject{
				projectResource("test", false, time.Time{}),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-8*time.Hour),
						time.Time{}),
				}),
				rcResource("rc2", "rc2", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-8*time.Hour),
						time.Time{}),
				}),
				podResource("pod1", "pod1", "test", "1",
					resource.MustParse("1G"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
					}),
			},
			ExpectedOpenshiftActions: []action{
				{Verb: "list", Resource: "deploymentconfigs"},
			},
			ExpectedKubeActions: []action{
				{Verb: "get", Resource: "namespaces", Name: "test"},
				{Verb: "list", Resource: "replicationcontrollers"},
				{Verb: "create", Resource: "resourcequotas", Name: "force-sleep"},
				{Verb: "delete", Resource: "pods", Name: "pod1"},
				{Verb: "delete", Resource: "pods", Name: "pod2"},
				{Verb: "list", Resource: "pods"},
				{Verb: "update", Resource: "namespaces", Name: "test"},
				{Verb: "update", Resource: "replicationcontrollers"},
				{Verb: "update", Resource: "replicationcontrollers"},
			},
		},
	}

	for name, test := range tests {
		t.Logf("Testing: %s", name)

		quota, err := time.ParseDuration(test.Quota)
		if err != nil {
			t.Logf("Error: %s", err)
		}
		period, err := time.ParseDuration(test.Period)
		if err != nil {
			t.Logf("Error: %s", err)
		}
		projectSleepPeriod, err := time.ParseDuration(test.ProjectSleepPeriod)
		if err != nil {
			t.Logf("Error: %s", err)
		}

		oc := fakeoclientset.NewSimpleClientset()
		kc := &fakekclientset.Clientset{}
		config := &SleeperConfig{
			Quota:              quota,
			Period:             period,
			ProjectSleepPeriod: projectSleepPeriod,
			TermQuota:          resource.MustParse(test.TermQuota),
			NonTermQuota:       resource.MustParse(test.NonTermQuota),
			DryRun:             test.DryRun,
			QuotaClient:        kc.CoreV1(),
		}
		kc.AddReactor("list", "namespaces", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
			list := &corev1.NamespaceList{}
			for i := range test.Projects {
				list.Items = append(list.Items, *test.Projects[i])
			}
			return true, list, nil
		})
		kc.AddReactor("list", "resourcequotas", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
			list := &corev1.ResourceQuotaList{}
			for i := range test.ResourceQuotas {
				list.Items = append(list.Items, *test.ResourceQuotas[i])
			}
			return true, list, nil
		})
		kc.AddReactor("get", "resourcequotas", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
			for i := range test.ResourceQuotas {
				if test.ResourceQuotas[i].Name == action.(ktesting.GetAction).GetName() {
					return true, test.ResourceQuotas[i], nil
				}
			}
			return true, nil, nil
		})
		kc.AddReactor("list", "pods", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
			list := &corev1.PodList{}
			for i := range test.Pods {
				list.Items = append(list.Items, *test.Pods[i])
			}
			return true, list, nil
		})
		kc.AddReactor("list", "replicationcontrollers", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
			list := &corev1.ReplicationControllerList{}
			for i := range test.ReplicationControllers {
				list.Items = append(list.Items, *test.ReplicationControllers[i])
			}
			return true, list, nil
		})

		oc.AddReactor("list", "deploymentconfigs", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
			list := &appsv1.DeploymentConfigList{}
			for i := range test.DeploymentConfigs {
				list.Items = append(list.Items, *test.DeploymentConfigs[i])
			}
			return true, list, nil
		})

		clientConfig := &restclient.Config{
			Host: "127.0.0.1",
			ContentConfig: restclient.ContentConfig{GroupVersion: &corev1.SchemeGroupVersion,
				NegotiatedSerializer: cache.Codecs},
		}
		// TODO FIX THIS...
		rcache := cache.NewResourceStore(oc, kc, clientConfig, nil)
		s := NewSleeper(config, rcache)

		projects, err := s.resourceStore.ProjectList.List(labels.Everything())
		if err != nil {
			t.Logf("Error: %s", err)
		}
		for _, project := range projects {
			s.syncProject(project)
		}

		// Test kubeClient actions
		for _, action := range kc.Actions() {
			switch a := action.(type) {
			case ktesting.ListActionImpl:
			case ktesting.UpdateActionImpl:
				found := false
				for _, e := range test.ExpectedKubeActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource().Resource {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action: %#v", action)
				}

			case ktesting.CreateActionImpl:
				accessor, _ := meta.Accessor(a.GetObject())
				found := false
				for _, e := range test.ExpectedKubeActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource().Resource && e.Name == accessor.GetName() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action (name=%s): %#v", accessor.GetName(), action)
				}

			case ktesting.DeleteActionImpl:
				found := false
				for _, e := range test.ExpectedKubeActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource().Resource && e.Name == a.GetName() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action (name=%s): %#v", a.GetName(), action)
				}

			case ktesting.GetActionImpl:
				found := false
				for _, e := range test.ExpectedKubeActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource().Resource && e.Name == a.GetName() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action (name=%s): %#v", a.GetName(), action)
				}

			default:
				t.Fatalf("unexpected action: %#v", action)
			}
		}

		// Test osClient actions
		for _, action := range oc.Actions() {
			switch a := action.(type) {
			case ktesting.ListActionImpl:
			case ktesting.UpdateActionImpl:
				found := false
				for _, e := range test.ExpectedOpenshiftActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource().Resource {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action: %#v", action)
				}

			case ktesting.CreateActionImpl:
				accessor, _ := meta.Accessor(a.GetObject())
				found := false
				for _, e := range test.ExpectedOpenshiftActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource().Resource && e.Name == accessor.GetName() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action (name=%s): %#v", accessor.GetName(), action)
				}

			case ktesting.DeleteActionImpl:
				found := false
				for _, e := range test.ExpectedOpenshiftActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource().Resource && e.Name == a.GetName() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action (name=%s): %#v", a.GetName(), action)
				}

			case ktesting.GetActionImpl:
				found := false
				for _, e := range test.ExpectedOpenshiftActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource().Resource && e.Name == a.GetName() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action (name=%s): %#v", a.GetName(), action)
				}

			default:
				t.Fatalf("unexpected action: %#v", action)
			}
		}

		// Check for missing expected actions
		for _, e := range test.ExpectedOpenshiftActions {
			found := false
			for _, action := range oc.Actions() {
				if action.GetVerb() != e.Verb {
					continue
				}
				if action.GetResource().Resource != e.Resource {
					continue
				}
				switch a := action.(type) {
				case ktesting.ListActionImpl:
					if e.Verb == action.GetVerb() && e.Resource == action.GetResource().Resource {
						found = true
					}
				case ktesting.UpdateActionImpl:
					if e.Verb == action.GetVerb() && e.Resource == action.GetResource().Resource {
						found = true
					}
				case ktesting.CreateActionImpl:
					accessor, _ := meta.Accessor(a.GetObject())
					if e.Name == accessor.GetName() {
						found = true
					}
				case ktesting.GetActionImpl:
					if e.Name == a.GetName() {
						found = true
					}
				case ktesting.DeleteActionImpl:
					if e.Name == a.GetName() {
						found = true
					}
				default:
				}
			}
			if !found {
				t.Logf("expected but didn't observe OpenShift action: %#v", e)
				t.Logf("observed OpenShift actions:")
				for _, a := range oc.Actions() {
					t.Logf("%#v", a)
				}
				t.FailNow()
			}
		}

		for _, e := range test.ExpectedKubeActions {
			found := false
			for _, action := range kc.Actions() {
				if action.GetVerb() != e.Verb {
					continue
				}
				if action.GetResource().Resource != e.Resource {
					continue
				}
				switch a := action.(type) {
				case ktesting.ListActionImpl:
					if e.Verb == action.GetVerb() && e.Resource == action.GetResource().Resource {
						found = true
					}
				case ktesting.CreateActionImpl:
					accessor, _ := meta.Accessor(a.GetObject())
					if e.Name == accessor.GetName() {
						found = true
					}
				case ktesting.UpdateActionImpl:
					if e.Verb == action.GetVerb() && e.Resource == action.GetResource().Resource {
						found = true
					}
				case ktesting.GetActionImpl:
					if e.Name == a.GetName() {
						found = true
					}
				case ktesting.DeleteActionImpl:
					if e.Name == a.GetName() {
						found = true
					}
				default:
				}
			}
			if !found {
				t.Logf("expected but didn't observe Kubernetes action: %#v", e)
				t.Logf("observed Kubernetes actions:")
				for _, a := range kc.Actions() {
					t.Logf("%#v", a)
				}
				t.FailNow()
			}
		}
	}
}

func project(name string) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			Kind: "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
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

func quota(name, namespace, memory, pods string) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		TypeMeta: metav1.TypeMeta{
			Kind: "resourcequotas",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList(map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceMemory: resource.MustParse(memory),
				corev1.ResourcePods:   resource.MustParse(pods),
			}),
		},
	}
}

func runningTime(start, end time.Time) *cache.RunningTime {
	return &cache.RunningTime{
		Start: start,
		End:   end,
	}
}
