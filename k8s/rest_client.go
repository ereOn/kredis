package k8s

import (
	"github.com/ereOn/k8s-redis-cluster-operator/tpr"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/unversioned"
	"k8s.io/client-go/pkg/runtime"
	"k8s.io/client-go/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
)

// Group is the group of the third-party resource.
const Group = "freelan.org"

// Version is the version of the third-party resource.
const Version = "v1"

// NewRESTClient initializes a new REST client that is able to manipulate the
// third-party resources.
func NewRESTClient(config *rest.Config) (*rest.RESTClient, error) {
	config.GroupVersion = &unversioned.GroupVersion{
		Group:   Group,
		Version: Version,
	}
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: api.Codecs}

	schemeBuilder := runtime.NewSchemeBuilder(addKnownTypes)
	schemeBuilder.AddToScheme(api.Scheme)

	return rest.RESTClientFor(config)
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(
		unversioned.GroupVersion{Group: Group, Version: Version},
		&tpr.RedisCluster{},
		&tpr.RedisClusterList{},
		&api.ListOptions{},
	)

	return nil
}
