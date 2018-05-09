package forcesleep

import (
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"strconv"
)

// Custom sorting for prioritizing projects in force-sleep
type Projects []*corev1.Namespace

func (p Projects) Len() int {
	return len(p)
}
func (p Projects) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p Projects) Less(i, j int) bool {
	p1 := p[i]
	p2 := p[j]
	// TODO: IF NO ANNOTATIONS YET, SHOULD SET TO "1" and "0"?
	var p1SortIndex, p2SortIndex string
	if p1.Annotations[ProjectSortAnnotation] == "" {
		p1SortIndex = "1"
	} else {
		p1SortIndex = p1.ObjectMeta.Annotations[ProjectSortAnnotation]
	}
	p1SortInt, err := strconv.Atoi(p1SortIndex)
	if err != nil {
		glog.Errorf("Force-sleeper: %v", err)
	}
	if p2.Annotations[ProjectSortAnnotation] == "" {
		p2SortIndex = "0"
	} else {
		p2SortIndex = p2.ObjectMeta.Annotations[ProjectSortAnnotation]
	}
	p2SortInt, err := strconv.Atoi(p2SortIndex)
	if err != nil {
		glog.Errorf("Force-sleeper: %v", err)
	}
	return p1SortInt < p2SortInt
}

func getQuotaSeconds(seconds float64, request, limit resource.Quantity) float64 {
	requestVal := float64(request.Value())
	limitVal := float64(limit.Value())
	var percentage float64
	percentage = requestVal / limitVal
	return seconds * percentage
}
