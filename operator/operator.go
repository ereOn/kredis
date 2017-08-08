package operator

import (
	"context"

	"github.com/ereOn/k8s-redis-cluster-operator/client"
	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	"github.com/go-kit/kit/log"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/tools/cache"
)

// An Operator manages Redis cluster resources in a Kubernetes Cluster.
//
// There should be only one operator running at any given time in a kubernetes
// cluster.
type Operator struct {
	clientset                 *client.Clientset
	logger                    log.Logger
	invalidRedisClusters      queue
	unmarkedRedisClusters     queue
	initializingRedisClusters queue
	redisClustersStore        cache.Store
	redisClustersController   cache.Controller
	replicasetsStore          cache.Store
	replicasetsController     cache.Controller
}

type queue struct {
	Name  string
	Items []*crd.RedisCluster
}

func (q queue) String() string {
	return q.Name
}

func (q *queue) Add(redisCluster *crd.RedisCluster) {
	q.Items = append(q.Items, redisCluster)
}

func (q *queue) Remove(redisCluster *crd.RedisCluster) {
	for i, rc := range q.Items {
		if redisCluster.Name == rc.Name {
			q.Items = append(q.Items[:i], q.Items[i+1:]...)
		}
	}
}

// New create a new operator.
func New(clientset *client.Clientset, logger log.Logger) *Operator {
	o := &Operator{
		clientset:                 clientset,
		logger:                    logger,
		invalidRedisClusters:      queue{Name: "invalid"},
		unmarkedRedisClusters:     queue{Name: "unmarked"},
		initializingRedisClusters: queue{Name: "initializing"},
	}

	source := cache.NewListWatchFromClient(
		o.clientset.RedisClustersClient,
		crd.RedisClusterDefinitionName,
		apiv1.NamespaceAll,
		fields.Everything(),
	)

	o.redisClustersStore, o.redisClustersController = cache.NewInformer(
		source,
		&crd.RedisCluster{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    o.onAdd,
			UpdateFunc: o.onUpdate,
			DeleteFunc: o.onDelete,
		},
	)

	source = cache.NewListWatchFromClient(
		o.clientset.ExtensionsV1beta1().RESTClient(),
		"replicasets",
		apiv1.NamespaceAll,
		fields.Everything(),
	)

	o.replicasetsStore, o.replicasetsController = cache.NewInformer(
		source,
		&v1beta1.ReplicaSet{},
		0,
		cache.ResourceEventHandlerFuncs{},
	)

	return o
}

// Run start the operator for as long as the specified context lives.
func (o *Operator) Run(ctx context.Context) {
	go o.replicasetsController.Run(ctx.Done())
	o.redisClustersController.Run(ctx.Done())
}

func (o *Operator) getQueue(redisCluster *crd.RedisCluster) *queue {
	switch redisCluster.Status.State {
	case "":
		return &o.unmarkedRedisClusters
	case crd.RedisClusterStateInitializing:
		return &o.initializingRedisClusters
	default:
		return &o.invalidRedisClusters
	}
}

func (o *Operator) onAdd(new interface{}) {
	newRedisCluster := new.(*crd.RedisCluster)
	newQueue := o.getQueue(newRedisCluster)

	newQueue.Add(newRedisCluster)
	o.logger.Log("event", "add", "redis-cluster", newRedisCluster.Name, "queue", newQueue)

	o.synchronize()
}

func (o *Operator) onUpdate(old interface{}, new interface{}) {
	oldRedisCluster := old.(*crd.RedisCluster)
	oldQueue := o.getQueue(oldRedisCluster)
	newRedisCluster := new.(*crd.RedisCluster)
	newQueue := o.getQueue(newRedisCluster)

	if oldQueue != newQueue {
		oldQueue.Remove(oldRedisCluster)
		newQueue.Add(newRedisCluster)
		o.logger.Log("event", "update", "redis-cluster", oldRedisCluster.Name, "old-queue", oldQueue, "new-queue", newQueue)
	}

	o.synchronize()
}

func (o *Operator) onDelete(old interface{}) {
	oldRedisCluster := old.(*crd.RedisCluster)
	oldQueue := o.getQueue(oldRedisCluster)

	oldQueue.Remove(oldRedisCluster)
	o.logger.Log("event", "delete", "redis-cluster", oldRedisCluster.Name, "queue", oldQueue)

	o.synchronize()
}

func (o *Operator) synchronize() {
	var operations []Operation

	for _, redisCluster := range o.unmarkedRedisClusters.Items {
		operations = append(operations, SetStatus(o.clientset.RedisClustersClient, redisCluster, crd.RedisClusterStateInitializing, "Initializing Redis cluster"))
	}

	for _, operation := range operations {
		if err := operation(); err != nil {
			o.logger.Log("event", "operation failure", "error", err)
		}
	}
}
