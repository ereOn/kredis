package k8s

import (
	"fmt"
	"sync"

	"github.com/ereOn/k8s-redis-cluster-operator/tpr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/errors"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/pkg/fields"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// Clientset represents a set of Kubernetes client.
type Clientset struct {
	RESTClient *rest.RESTClient
	Clientset  *kubernetes.Clientset
	watchStop  chan struct{}
	close      sync.Once
}

// NewForConfig creates a new client from the specified configuration.
func NewForConfig(config *rest.Config) (*Clientset, error) {
	restClient, err := NewRESTClient(config)

	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)

	if err != nil {
		return nil, err
	}

	return &Clientset{
		RESTClient: restClient,
		Clientset:  clientset,
		watchStop:  make(chan struct{}),
	}, nil
}

// RedisClusterResourceName is the name of the Redis cluster resource.
var RedisClusterResourceName = "redis-cluster." + Group

// RedisClusterResourceDescription is the description of the Redis cluster
// resource.
var RedisClusterResourceDescription = "A Redis cluster."

// RegisterThirdPartyResources registers the third-party resources.
func (c *Clientset) RegisterThirdPartyResources() error {
	// initialize third party resources if they do not exist
	_, err := c.Clientset.Extensions().ThirdPartyResources().Get(RedisClusterResourceName)

	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}

		tpr := &v1beta1.ThirdPartyResource{
			ObjectMeta: v1.ObjectMeta{
				Name: RedisClusterResourceName,
			},
			Versions: []v1beta1.APIVersion{
				{Name: Version},
			},
			Description: RedisClusterResourceDescription,
		}

		_, err = c.Clientset.Extensions().ThirdPartyResources().Create(tpr)
	}

	return err
}

// Close closes the clientset.
func (c *Clientset) Close() {
	c.close.Do(func() {
		close(c.watchStop)
		c.watchStop = nil
	})
}

// Watch watches the third-party resources and reacts.
func (c *Clientset) Watch() {
	source := cache.NewListWatchFromClient(c.RESTClient, "redisclusters", api.NamespaceAll, fields.Everything())

	_, controller := cache.NewInformer(
		source,
		&tpr.RedisCluster{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(new interface{}) {
				fmt.Println("add", new)
			},
			UpdateFunc: func(old interface{}, new interface{}) {
				fmt.Println("update", old, new)
			},
			DeleteFunc: func(old interface{}) {
				fmt.Println("delete", old)
			},
		},
	)

	controller.Run(c.watchStop)
}
