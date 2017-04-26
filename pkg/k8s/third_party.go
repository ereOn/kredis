package k8s

import (
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/rest"
)

// RedisClusterResource is the "Redis Cluster" resource.
var RedisClusterResource = "redis-cluster"

// ResourceSuffix is the "Redis Cluster" resource name.
var ResourceSuffix = "freelan.org"

// RedisClusterResourceName is the "Redis Cluster" resource name.
var RedisClusterResourceName = RedisClusterResource + "." + ResourceSuffix

// RegisterRedisClusterResource makes sure the third-party resource is registered.
func RegisterRedisClusterResource(clientset *kubernetes.Clientset) (bool, error) {
	tpr, err := GetRedisClusterResource(clientset)

	if err != nil {
		return false, err
	}

	if tpr != nil {
		return false, nil
	}

	_, err = CreateRedisClusterResource(clientset)

	return true, err
}

// UnregisterRedisClusterResource makes sure the third-party resource is unregistered.
func UnregisterRedisClusterResource(clientset *kubernetes.Clientset) error {
	tpr, err := GetRedisClusterResource(clientset)

	if err != nil {
		return err
	}

	if tpr != nil {
		return DeleteRedisClusterResource(clientset)
	}

	return nil
}

// GetRedisClusterResource returns the third-party resource if it was registered
// or nil otherwise.
func GetRedisClusterResource(clientset *kubernetes.Clientset) (*v1beta1.ThirdPartyResource, error) {
	tpr, err := clientset.ExtensionsV1beta1().ThirdPartyResources().Get(RedisClusterResourceName, metav1.GetOptions{})

	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	return tpr, nil
}

// CreateRedisClusterResource creates the third-party resource.
func CreateRedisClusterResource(clientset *kubernetes.Clientset) (*v1beta1.ThirdPartyResource, error) {
	tpr := &v1beta1.ThirdPartyResource{
		ObjectMeta: metav1.ObjectMeta{
			Name: RedisClusterResourceName,
		},
		Versions: []v1beta1.APIVersion{
			{Name: "v1"},
		},
		Description: "A Redis Cluster",
	}

	return clientset.ExtensionsV1beta1().ThirdPartyResources().Create(tpr)
}

// DeleteRedisClusterResource deletes the third-party resource.
func DeleteRedisClusterResource(clientset *kubernetes.Clientset) error {
	return clientset.ExtensionsV1beta1().ThirdPartyResources().Delete(RedisClusterResourceName, &metav1.DeleteOptions{})
}

// RedisClusterSpec represents a Redis Cluster resource specification.
type RedisClusterSpec struct {
	NodesCount int `json:"nodesCount"`
}

// RedisCluster represents a Redis Cluster instance.
type RedisCluster struct {
	metav1.TypeMeta `json:",inline"`
	Metadata        metav1.ObjectMeta `json:"metadata"`
	Spec            RedisClusterSpec  `json:"spec"`
}

// GetObjectKind is required to satisfy Ojbect interface
func (rc *RedisCluster) GetObjectKind() schema.ObjectKind {
	return &rc.TypeMeta
}

// GetObjectMeta is required to satisfy ObjectMetaAccessor interface
func (rc *RedisCluster) GetObjectMeta() metav1.Object {
	return &rc.Metadata
}

func ConfigureClient(config *rest.Config) {
	groupversion := schema.GroupVersion{
		Group:   ResourceSuffix,
		Version: "v1",
	}

	config.GroupVersion = &groupversion
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: api.Codecs}

	schemeBuilder := runtime.NewSchemeBuilder(
		func(scheme *runtime.Scheme) error {
			scheme.AddKnownTypes(
				groupversion,
				&RedisCluster{},
				&RedisCluster{},
			)
			return nil
		},
	)
	metav1.AddToGroupVersion(api.Scheme, groupversion)
	schemeBuilder.AddToScheme(api.Scheme)
}

// CreateRedisCluster creates a Redis Cluster.
func CreateRedisCluster(client *rest.RESTClient, namespace string, redisCluster *RedisCluster) (RedisCluster, error) {
	var body runtime.Object = redisCluster
	var result RedisCluster

	err := client.Post().
		Resource("redisclusters").
		Namespace(namespace).
		Body(body).
		Do().Into(&result)

	return result, err
}
