package operator

import (
	"context"
	"encoding/json"

	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	"github.com/go-kit/kit/log"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// An Operator manages Redis cluster resources in a Kubernetes Cluster.
//
// There should be only one operator running at any given time in a kubernetes
// cluster.
type Operator struct {
	client                    *rest.RESTClient
	logger                    log.Logger
	invalidRedisClusters      queue
	unmarkedRedisClusters     queue
	initializingRedisClusters queue
	store                     cache.Store
	controller                cache.Controller
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
func New(client *rest.RESTClient, logger log.Logger) *Operator {
	o := &Operator{
		client:                    client,
		logger:                    logger,
		invalidRedisClusters:      queue{Name: "invalid"},
		unmarkedRedisClusters:     queue{Name: "unmarked"},
		initializingRedisClusters: queue{Name: "initializing"},
	}

	source := cache.NewListWatchFromClient(
		o.client,
		crd.RedisClusterDefinitionName,
		apiv1.NamespaceAll,
		fields.Everything(),
	)

	o.store, o.controller = cache.NewInformer(
		source,
		&crd.RedisCluster{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    o.onAdd,
			UpdateFunc: o.onUpdate,
			DeleteFunc: o.onDelete,
		},
	)

	return o
}

// Run start the operator for as long as the specified context lives.
func (o *Operator) Run(ctx context.Context) {
	o.controller.Run(ctx.Done())
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
	for _, redisCluster := range o.unmarkedRedisClusters.Items {
		// TODO: Refactor this, handle errors and move to operations.go
		body, err := json.Marshal(struct {
			Status crd.RedisClusterStatus `json:"status"`
		}{
			Status: crd.RedisClusterStatus{
				State:   crd.RedisClusterStateInitializing,
				Message: "Initializing Redis cluster.",
			},
		})

		if err != nil {
			o.logger.Log("event", "serialize", "error", err)
			continue
		}

		if err = o.client.
			Patch(types.MergePatchType).
			Resource(crd.RedisClusterDefinitionName).
			Namespace(redisCluster.Namespace).
			Name(redisCluster.Name).
			Body(body).
			Do().
			Error(); err != nil {
			o.logger.Log("event", "foo", "error", err)
		}
	}
}
