package forcesleep

import (
	"github.com/openshift/online-hibernation/pkg/cache"

	"github.com/openshift/origin/pkg/cmd/util/clientcmd"

	"github.com/spf13/pflag"

	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/kubectl"
	ctlresource "k8s.io/kubernetes/pkg/kubectl/resource"
	"k8s.io/kubernetes/pkg/runtime"
)

// Custom sorting for prioritizing projects in force-sleep
type Projects []interface{}

func (p Projects) Len() int {
	return len(p)
}
func (p Projects) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p Projects) Less(i, j int) bool {
	p1 := p[i].(*cache.ResourceObject)
	p2 := p[j].(*cache.ResourceObject)
	return p1.ProjectSortIndex < p2.ProjectSortIndex
}

func (s *Sleeper) createSleepResources() (runtime.Object, error) {
	quotaGenerator := &kubectl.ResourceQuotaGeneratorV1{
		Name: ProjectSleepQuotaName,
		Hard: "pods=0",
	}
	obj, err := quotaGenerator.StructuredGenerate()
	if err != nil {
		return nil, err
	}
	f := clientcmd.New(pflag.NewFlagSet("empty", pflag.ContinueOnError))

	mapper, typer := f.Object(false)
	resourceMapper := &ctlresource.Mapper{
		ObjectTyper:  typer,
		RESTMapper:   mapper,
		ClientMapper: ctlresource.ClientMapperFunc(f.ClientForMapping),
	}
	info, err := resourceMapper.InfoForObject(obj, nil)
	if err != nil {
		return nil, err
	}
	if err := kubectl.UpdateApplyAnnotation(info, f.JSONEncoder()); err != nil {
		return nil, err
	}
	return info.Object, nil
}

func getQuotaSeconds(seconds float64, request, limit resource.Quantity) float64 {
	requestVal := float64(request.Value())
	limitVal := float64(limit.Value())
	var percentage float64
	percentage = requestVal / limitVal
	return seconds * percentage
}
