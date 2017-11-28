package forcesleep

import (
	"github.com/openshift/online-hibernation/pkg/cache"

	"k8s.io/apimachinery/pkg/api/resource"
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

func getQuotaSeconds(seconds float64, request, limit resource.Quantity) float64 {
	requestVal := float64(request.Value())
	limitVal := float64(limit.Value())
	var percentage float64
	percentage = requestVal / limitVal
	return seconds * percentage
}
