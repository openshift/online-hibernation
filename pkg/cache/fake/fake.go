package fake

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest/fake"

	"github.com/emicklei/go-restful-swagger12"
	"github.com/googleapis/gnostic/OpenAPIv2"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/version"
	restclient "k8s.io/client-go/rest"
)

type FakeCachedDiscoveryInterface struct {
	invalidateCalls int
	fresh           bool
	enabledA        bool
}

var _ discovery.CachedDiscoveryInterface = &FakeCachedDiscoveryInterface{}

func (c *FakeCachedDiscoveryInterface) Fresh() bool {
	return c.fresh
}

func (c *FakeCachedDiscoveryInterface) Invalidate() {
	c.invalidateCalls = c.invalidateCalls + 1
	c.fresh = true
	c.enabledA = true
}

func (c *FakeCachedDiscoveryInterface) RESTClient() restclient.Interface {
	return &fake.RESTClient{}
}
func (c *FakeCachedDiscoveryInterface) ServerGroups() (*metav1.APIGroupList, error) {
	if c.enabledA {
		return &metav1.APIGroupList{
			Groups: []metav1.APIGroup{
				{
					Name: "a",
					Versions: []metav1.GroupVersionForDiscovery{
						{
							GroupVersion: "a/v1",
							Version:      "v1",
						},
					},
					PreferredVersion: metav1.GroupVersionForDiscovery{
						GroupVersion: "a/v1",
						Version:      "v1",
					},
				},
			},
		}, nil
	}
	return &metav1.APIGroupList{}, nil
}

func (c *FakeCachedDiscoveryInterface) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	if c.enabledA && groupVersion == "a/v1" {
		return &metav1.APIResourceList{
			GroupVersion: "a/v1",
			APIResources: []metav1.APIResource{
				{
					Name:       "athing",
					Kind:       "Athing",
					Namespaced: false,
				},
			},
		}, nil
	}

	return nil, errors.NewNotFound(schema.GroupResource{}, "")
}

func (c *FakeCachedDiscoveryInterface) ServerResources() ([]*metav1.APIResourceList, error) {
	if c.enabledA {
		av1, _ := c.ServerResourcesForGroupVersion("a/v1")
		return []*metav1.APIResourceList{av1}, nil
	}
	return []*metav1.APIResourceList{}, nil
}

func (c *FakeCachedDiscoveryInterface) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	if c.enabledA {
		return []*metav1.APIResourceList{
			{
				GroupVersion: "a/v1",
				APIResources: []metav1.APIResource{
					{
						Name:  "athing",
						Kind:  "Athing",
						Verbs: []string{},
					},
				},
			},
		}, nil
	}
	return nil, nil
}

func (c *FakeCachedDiscoveryInterface) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return nil, nil
}

func (c *FakeCachedDiscoveryInterface) ServerVersion() (*version.Info, error) {
	return &version.Info{}, nil
}

func (c *FakeCachedDiscoveryInterface) SwaggerSchema(version schema.GroupVersion) (*swagger.ApiDeclaration, error) {
	return &swagger.ApiDeclaration{}, nil
}

func (c *FakeCachedDiscoveryInterface) OpenAPISchema() (*openapi_v2.Document, error) {
	return &openapi_v2.Document{}, nil
}
