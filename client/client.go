package client

import (
	"fmt"

	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Clientset is the client interface.
type Clientset struct {
	*kubernetes.Clientset
	APIExtensionsClientset *apiextensionsclient.Clientset
	RedisClustersClient    *rest.RESTClient
}

// New creates a new client for managing Redis Clusters resources.
func New(cfg *rest.Config) (*rest.RESTClient, *runtime.Scheme, error) {
	scheme := runtime.NewScheme()

	if err := crd.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}

	config := *cfg
	config.GroupVersion = &crd.SchemeGroupVersion
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: serializer.NewCodecFactory(scheme)}

	client, err := rest.RESTClientFor(&config)

	if err != nil {
		return nil, nil, err
	}

	return client, scheme, nil
}

// NewForConfig creates a new clientset from a specified configuration.
func NewForConfig(config *rest.Config) (*Clientset, error) {
	clientset, err := kubernetes.NewForConfig(config)

	if err != nil {
		return nil, fmt.Errorf("failed to instantiate clientset: %s", err)
	}

	apiextensionsClientset, err := apiextensionsclient.NewForConfig(config)

	if err != nil {
		return nil, fmt.Errorf("failed to instantiate API extensions clientset: %s", err)
	}

	redisClustersClient, _, err := New(config)

	if err != nil {
		return nil, fmt.Errorf("failed to instantiate Redis cluster client: %s", err)
	}

	return &Clientset{
		Clientset:              clientset,
		APIExtensionsClientset: apiextensionsClientset,
		RedisClustersClient:    redisClustersClient,
	}, nil
}
