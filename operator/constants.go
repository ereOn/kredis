package operator

import "github.com/ereOn/k8s-redis-cluster-operator/crd"

const (
	// CreatedByAnnotation is the annotation set on ReplicaSet to indicate
	// which Redis cluster caused its creation.
	CreatedByAnnotation = crd.RedisClusterDefinitionGroup + "/created-by"
	// PrimaryIndexAnnotation is the annotation set on ReplicaSet to indicate
	// which Redis instance index the ReplicaSet provides.
	PrimaryIndexAnnotation = crd.RedisClusterDefinitionGroup + "/primary-index"
	// SecondaryIndexAnnotation is the annotation set on ReplicaSet to indicate
	// which Redis slave index the ReplicaSet provides.
	SecondaryIndexAnnotation = crd.RedisClusterDefinitionGroup + "/secondary-index"
)
