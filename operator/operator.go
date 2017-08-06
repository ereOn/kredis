package operator

import (
	"context"

	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	"github.com/go-kit/kit/log"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// An Operator manages Redis cluster resources in a Kubernetes Cluster.
//
// There should be only one operator running at any given time in a kubernetes
// cluster.
type Operator struct {
	client                 *rest.RESTClient
	logger                 log.Logger
	uncreatedRedisClusters []crd.RedisCluster
}

// New create a new operator.
func New(client *rest.RESTClient, logger log.Logger) *Operator {
	return &Operator{
		client: client,
		logger: logger,
	}
}

// Run start the operator for as long as the specified context lives.
func (o *Operator) Run(ctx context.Context) {
	source := cache.NewListWatchFromClient(
		o.client,
		crd.RedisClusterDefinitionName,
		apiv1.NamespaceAll,
		fields.Everything(),
	)

	_, controller := cache.NewInformer(
		source,
		&crd.RedisCluster{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    o.onAdd,
			UpdateFunc: o.onUpdate,
			DeleteFunc: o.onDelete,
		},
	)

	controller.Run(ctx.Done())
}

func (o *Operator) onAdd(new interface{}) {
	newRedisCluster := new.(*crd.RedisCluster)

	o.logger.Log("event", "add", "redis-cluster", newRedisCluster.Name)
}

func (o *Operator) onUpdate(old interface{}, new interface{}) {
	oldRedisCluster := old.(*crd.RedisCluster)
	//newRedisCluster := new.(*crd.RedisCluster)

	o.logger.Log("event", "update", "redis-cluster", oldRedisCluster.Name)
}

func (o *Operator) onDelete(old interface{}) {
	oldRedisCluster := old.(*crd.RedisCluster)

	o.logger.Log("event", "delete", "redis-cluster", oldRedisCluster.Name)
}
