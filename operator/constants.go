package operator

import "github.com/ereOn/k8s-redis-cluster-operator/crd"

const (
	// CreatedByAnnotation is the annotation set on StatefulSets to indicate
	// which Redis cluster caused their creation.
	CreatedByAnnotation = crd.RedisClusterDefinitionGroup + "/created-by"
	// ServiceAnnotation is the annotation set on RedisClusters to indicate
	// which service object they are responsible of.
	ServiceAnnotation = crd.RedisClusterDefinitionGroup + "/service"
	// ServiceAnnotation is the annotation set on RedisClusters to indicate
	// which service object version they last created.
	ServiceVersionAnnotation = crd.RedisClusterDefinitionGroup + "/service-version"
)
