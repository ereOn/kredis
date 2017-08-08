package operator

import "github.com/ereOn/k8s-redis-cluster-operator/crd"

const (
	// CreatedByAnnotation is the annotation set on StatefulSets to indicate
	// which Redis cluster caused their creation.
	CreatedByAnnotation = crd.RedisClusterDefinitionGroup + "/created-by"
)
