package forcesleep

import (
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"

	"github.com/openshift/origin/pkg/client/testclient"
	"github.com/openshift/origin/pkg/cmd/util/clientcmd"
	deployapi "github.com/openshift/origin/pkg/deploy/api"

	"github.com/spf13/pflag"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/api/unversioned"
	ktestclient "k8s.io/kubernetes/pkg/client/unversioned/testclient"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/types"
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
		Projects                 []string
		DeploymentConfigs        []*deployapi.DeploymentConfig
		Pods                     []*kapi.Pod
		ReplicationControllers   []*kapi.ReplicationController
		ResourceQuotas           []*kapi.ResourceQuota
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
			Projects:           []string{"test"},
			ResourceQuotas: []*kapi.ResourceQuota{
				quota("compute-resources", "test", "1G", "1"),
			},
			Pods: []*kapi.Pod{
				pod("pod1", "test"),
			},
			ReplicationControllers: []*kapi.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*deployapi.DeploymentConfig{
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
				{Verb: "update", Resource: "deploymentconfigs"},
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
			Projects:           []string{"test"},
			ResourceQuotas: []*kapi.ResourceQuota{
				quota("compute-resources", "test", "1G", "1"),
			},
			Pods: []*kapi.Pod{
				pod("pod1", "test"),
			},
			ReplicationControllers: []*kapi.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*deployapi.DeploymentConfig{
				dc("dc1", "test"),
			},
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
			Projects:           []string{"test"},
			ResourceQuotas: []*kapi.ResourceQuota{
				quota("compute-resources", "test", "1G", "1"),
			},
			Pods: []*kapi.Pod{
				pod("pod1", "test"),
			},
			ReplicationControllers: []*kapi.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*deployapi.DeploymentConfig{
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
			Projects:           []string{"test"},
			ResourceQuotas: []*kapi.ResourceQuota{
				quota("compute-resources", "test", "1G", "1"),
				quota("force-sleep", "test", "1G", "0"),
			},
			Pods: []*kapi.Pod{},
			ReplicationControllers: []*kapi.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*deployapi.DeploymentConfig{
				dc("dc1", "test"),
			},
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
			Projects:           []string{"test"},
			ResourceQuotas: []*kapi.ResourceQuota{
				quota("compute-resources", "test", "1G", "1"),
				quota("force-sleep", "test", "1G", "0"),
			},
			Pods: []*kapi.Pod{},
			ReplicationControllers: []*kapi.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*deployapi.DeploymentConfig{
				dc("dc1", "test"),
			},
			Resources: []*cache.ResourceObject{
				projectResource("test", true, time.Now().Add(-4*time.Hour)),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-20*time.Hour),
						time.Now().Add(-4*time.Hour)),
				}),
			},
			ExpectedOpenshiftActions: []action{
				{Verb: "update", Resource: "deploymentconfigs"},
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
			Projects:           []string{"test"},
			ResourceQuotas: []*kapi.ResourceQuota{
				quota("compute-resources", "test", "1G", "2"),
			},
			Pods: []*kapi.Pod{
				pod("pod1", "test"),
				pod("pod2", "test"),
			},
			ReplicationControllers: []*kapi.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*deployapi.DeploymentConfig{
				dc("dc1", "test"),
			},
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
				{Verb: "update", Resource: "deploymentconfigs"},
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
			Projects:           []string{"test"},
			ResourceQuotas: []*kapi.ResourceQuota{
				quota("compute-resources", "test", "1G", "2"),
			},
			Pods: []*kapi.Pod{
				pod("pod1", "test"),
			},
			ReplicationControllers: []*kapi.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*deployapi.DeploymentConfig{
				dc("dc1", "test"),
			},
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
			Projects:           []string{"test"},
			ResourceQuotas: []*kapi.ResourceQuota{
				quota("compute-resources", "test", "1G", "2"),
			},
			Pods: []*kapi.Pod{},
			ReplicationControllers: []*kapi.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*deployapi.DeploymentConfig{
				dc("dc1", "test"),
			},
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
			Projects:           []string{"test"},
			ResourceQuotas: []*kapi.ResourceQuota{
				quota("compute-resources", "test", "1G", "1"),
			},
			Pods: []*kapi.Pod{
				pod("pod1", "test"),
			},
			ReplicationControllers: []*kapi.ReplicationController{
				rc("rc1", "test"),
			},
			DeploymentConfigs: []*deployapi.DeploymentConfig{
				dc("dc1", "test"),
			},
			Resources: []*cache.ResourceObject{
				projectResource("test", false, time.Time{}),
				rcResource("rc1", "rc1", "test", "1", "dc1", []*cache.RunningTime{
					runningTime(time.Now().Add(-16*time.Hour),
						time.Time{}),
				}),
				podResource("pod1", "pod1", "test", "1",
					resource.MustParse("1M"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
					}),
			},
			ExpectedOpenshiftActions: []action{},
			ExpectedKubeActions:      []action{
			//{Verb: "get", Resource: "replicationcontrollers"},
			//{Verb: "get", Resource: "replicationcontrollers"},
			},
		},

		"Scale DC that exceeds running limit (due to multiple active RCs)": {
			Quota:              "16h",
			Period:             "24h",
			ProjectSleepPeriod: "8h",
			TermQuota:          "1G",
			NonTermQuota:       "1G",
			DryRun:             false,
			Projects:           []string{"test"},
			ResourceQuotas: []*kapi.ResourceQuota{
				quota("compute-resources", "test", "1G", "2"),
			},
			Pods: []*kapi.Pod{
				pod("pod1", "test"),
				pod("pod2", "test"),
			},
			ReplicationControllers: []*kapi.ReplicationController{
				rc("rc1", "test"),
				rc("rc2", "test"),
			},
			DeploymentConfigs: []*deployapi.DeploymentConfig{
				dc("dc1", "test"),
			},
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
					resource.MustParse("1M"),
					[]*cache.RunningTime{
						runningTime(time.Now().Add(-16*time.Hour), time.Time{}),
					}),
			},
			ExpectedOpenshiftActions: []action{},
			ExpectedKubeActions:      []action{
			//{Verb: "get", Resource: "replicationcontrollers", Name: "rc1"},
			//{Verb: "get", Resource: "replicationcontrollers", Name: "rc2"},
			//{Verb: "update", Resource: "replicationcontrollers"},
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

		config := &SleeperConfig{
			Quota:              quota,
			Period:             period,
			ProjectSleepPeriod: projectSleepPeriod,
			TermQuota:          resource.MustParse(test.TermQuota),
			NonTermQuota:       resource.MustParse(test.NonTermQuota),
			DryRun:             test.DryRun,
		}
		kc := &ktestclient.Fake{}
		kc.AddReactor("list", "resourcequotas", func(action ktestclient.Action) (handled bool, ret runtime.Object, err error) {
			list := &kapi.ResourceQuotaList{}
			for i := range test.ResourceQuotas {
				list.Items = append(list.Items, *test.ResourceQuotas[i])
			}
			return true, list, nil
		})
		kc.AddReactor("get", "resourcequotas", func(action ktestclient.Action) (handled bool, ret runtime.Object, err error) {
			for i := range test.ResourceQuotas {
				if test.ResourceQuotas[i].Name == action.(ktestclient.GetAction).GetName() {
					return true, test.ResourceQuotas[i], nil
				}
			}
			return true, nil, nil
		})
		kc.AddReactor("list", "pods", func(action ktestclient.Action) (handled bool, ret runtime.Object, err error) {
			list := &kapi.PodList{}
			for i := range test.Pods {
				list.Items = append(list.Items, *test.Pods[i])
			}
			return true, list, nil
		})
		kc.AddReactor("list", "replicationcontrollers", func(action ktestclient.Action) (handled bool, ret runtime.Object, err error) {
			list := &kapi.ReplicationControllerList{}
			for i := range test.ReplicationControllers {
				list.Items = append(list.Items, *test.ReplicationControllers[i])
			}
			return true, list, nil
		})

		oc := &testclient.Fake{}
		oc.AddReactor("list", "deploymentconfigs", func(action ktestclient.Action) (handled bool, ret runtime.Object, err error) {
			list := &deployapi.DeploymentConfigList{}
			for i := range test.DeploymentConfigs {
				list.Items = append(list.Items, *test.DeploymentConfigs[i])
			}
			return true, list, nil
		})

		f := clientcmd.New(pflag.NewFlagSet("empty", pflag.ContinueOnError))
		excludeNamespaces := "default,logging,kube-system,openshift-infra"
		namespaces := strings.Split(excludeNamespaces, ",")
		exclude := make(map[string]bool)
		for _, name := range namespaces {
			exclude[name] = true
		}

		rcache := cache.NewCache(oc, kc, f, exclude)
		s := NewSleeper(config, f, rcache)

		for _, resource := range test.Resources {
			err := s.resources.Indexer.AddResourceObject(resource)
			if err != nil {
				t.Logf("Error: %s", err)
			}

		}

		for _, project := range test.Projects {
			s.syncProject(project)
		}

		// Test kubeClient actions
		for _, action := range kc.Actions() {
			switch a := action.(type) {
			case ktestclient.ListActionImpl:
			case ktestclient.UpdateActionImpl:
				found := false
				for _, e := range test.ExpectedKubeActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action: %#v", action)
				}

			case ktestclient.CreateActionImpl:
				accessor, _ := meta.Accessor(a.GetObject())
				found := false
				for _, e := range test.ExpectedKubeActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource() && e.Name == accessor.GetName() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action (name=%s): %#v", accessor.GetName(), action)
				}

			case ktestclient.DeleteActionImpl:
				found := false
				for _, e := range test.ExpectedKubeActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource() && e.Name == a.GetName() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action (name=%s): %#v", a.GetName(), action)
				}

			case ktestclient.GetActionImpl:
				found := false
				for _, e := range test.ExpectedKubeActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource() && e.Name == a.GetName() {
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
			case ktestclient.ListActionImpl:
			case ktestclient.UpdateActionImpl:
				found := false
				for _, e := range test.ExpectedOpenshiftActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action: %#v", action)
				}

			case ktestclient.CreateActionImpl:
				accessor, _ := meta.Accessor(a.GetObject())
				found := false
				for _, e := range test.ExpectedOpenshiftActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource() && e.Name == accessor.GetName() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action (name=%s): %#v", accessor.GetName(), action)
				}

			case ktestclient.DeleteActionImpl:
				found := false
				for _, e := range test.ExpectedOpenshiftActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource() && e.Name == a.GetName() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("unexpected action (name=%s): %#v", a.GetName(), action)
				}

			case ktestclient.GetActionImpl:
				found := false
				for _, e := range test.ExpectedOpenshiftActions {
					if e.Verb != a.GetVerb() {
						continue
					}
					if e.Resource == a.GetResource() && e.Name == a.GetName() {
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
				if action.GetResource() != e.Resource {
					continue
				}
				switch a := action.(type) {
				case ktestclient.ListActionImpl:
					if e.Verb == action.GetVerb() && e.Resource == action.GetResource() {
						found = true
					}
				case ktestclient.UpdateActionImpl:
					if e.Verb == action.GetVerb() && e.Resource == action.GetResource() {
						found = true
					}
				case ktestclient.CreateActionImpl:
					accessor, _ := meta.Accessor(a.GetObject())
					if e.Name == accessor.GetName() {
						found = true
					}
				case ktestclient.GetActionImpl:
					if e.Name == a.GetName() {
						found = true
					}
				case ktestclient.DeleteActionImpl:
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
				if action.GetResource() != e.Resource {
					continue
				}
				switch a := action.(type) {
				case ktestclient.ListActionImpl:
					if e.Verb == action.GetVerb() && e.Resource == action.GetResource() {
						found = true
					}
				case ktestclient.CreateActionImpl:
					accessor, _ := meta.Accessor(a.GetObject())
					if e.Name == accessor.GetName() {
						found = true
					}
				case ktestclient.UpdateActionImpl:
					if e.Verb == action.GetVerb() && e.Resource == action.GetResource() {
						found = true
					}
				case ktestclient.GetActionImpl:
					if e.Name == a.GetName() {
						found = true
					}
				case ktestclient.DeleteActionImpl:
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

func quota(name, namespace, memory, pods string) *kapi.ResourceQuota {
	return &kapi.ResourceQuota{
		TypeMeta: unversioned.TypeMeta{
			Kind: "resourcequotas",
		},
		ObjectMeta: kapi.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: kapi.ResourceQuotaSpec{
			Hard: kapi.ResourceList(map[kapi.ResourceName]resource.Quantity{
				kapi.ResourceMemory: resource.MustParse(memory),
				kapi.ResourcePods:   resource.MustParse(pods),
			}),
		},
	}
}

func podResource(uid, name, namespace, resourceVersion string, request resource.Quantity, rt []*cache.RunningTime) *cache.ResourceObject {
	return &cache.ResourceObject{
		UID:             types.UID(uid),
		Name:            name,
		Namespace:       namespace,
		Kind:            cache.PodKind,
		ResourceVersion: resourceVersion,
		MemoryRequested: request,
		RunningTimes:    rt,
		Terminating:     false,
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

func projectResource(name string, isAsleep bool, lastSleep time.Time) *cache.ResourceObject {
	return &cache.ResourceObject{
		UID:              types.UID(name),
		Name:             name,
		Namespace:        name,
		Kind:             cache.ProjectKind,
		LastSleepTime:    lastSleep,
		ProjectSortIndex: 0.0,
		IsAsleep:         isAsleep,
	}
}

func runningTime(start, end time.Time) *cache.RunningTime {
	return &cache.RunningTime{
		Start: start,
		End:   end,
	}
}
