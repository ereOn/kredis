package crd

import (
	"reflect"

	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// RedisClusterDefinitionName is the name of the Redis clusters CRD.
	RedisClusterDefinitionName = "redisclusters"
	// RedisClusterDefinitionNameSingular is the singular name of the Redis clusters CRD.
	RedisClusterDefinitionNameSingular = "rediscluster"
	// RedisClusterDefinitionGroup is the group of the Redis clusters CRD.
	RedisClusterDefinitionGroup = "freelan.org"
)

// RedisCluster represents a Redis cluster.
type RedisCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              RedisClusterSpec   `json:"spec"`
	Status            RedisClusterStatus `json:"status,omitempty"`
}

// RedisClusterSpec is the specification for Redis clusters.
type RedisClusterSpec struct {
	Instances uint `json:"instances"`
	Slaves    uint `json:"slaves"`
}

// RedisClusterStatus describes the status of a Redis cluster.
type RedisClusterStatus struct {
	State   RedisClusterState `json:"state,omitempty"`
	Message string            `json:"message,omitempty"`
}

// RedisClusterState describe the state of a Redis cluster.
type RedisClusterState string

// RedisClusterCRD is the CRD for Redis clusters.
var RedisClusterCRD = &apiextensionsv1beta1.CustomResourceDefinition{
	ObjectMeta: metav1.ObjectMeta{
		Name: RedisClusterDefinitionName + "." + RedisClusterDefinitionGroup,
	},
	Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
		Group:   RedisClusterDefinitionGroup,
		Version: "v1",
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
			Plural:   RedisClusterDefinitionName,
			Singular: RedisClusterDefinitionNameSingular,
			Kind:     reflect.TypeOf(RedisCluster{}).Name(),
		},
	},
}

// Register the CRD for Redis clusters.
func Register(clientset apiextensionsclient.Interface) (*apiextensionsv1beta1.CustomResourceDefinition, error) {

	return clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(RedisClusterCRD)
}

// Unregister the CRD for Redis clusters.
func Unregister(clientset apiextensionsclient.Interface) error {

	return clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(RedisClusterCRD.Name, nil)
}
