package operator

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ereOn/k8s-redis-cluster-operator/client"
	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	"github.com/go-kit/kit/log"

	"k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/tools/cache"
)

// An Operator manages Redis cluster resources in a Kubernetes Cluster.
//
// There should be only one operator running at any given time in a kubernetes
// cluster.
type Operator struct {
	lock                      sync.Mutex
	clientset                 *client.Clientset
	logger                    log.Logger
	invalidRedisClusters      queue
	unmarkedRedisClusters     queue
	initializingRedisClusters queue
	initializedRedisClusters  queue
	redisClustersStore        cache.Store
	redisClustersController   cache.Controller
	statefulSetsStore         cache.Store
	statefulSetsController    cache.Controller
}

type queue struct {
	Name  string
	Items []*crd.RedisCluster
}

func (q queue) String() string {
	return q.Name
}

func (q *queue) Add(redisCluster *crd.RedisCluster) bool {
	for _, rc := range q.Items {
		if redisCluster.Name == rc.Name {
			return false
		}
	}

	q.Items = append(q.Items, redisCluster)

	return true
}

func (q *queue) Remove(redisCluster *crd.RedisCluster) bool {
	for i, rc := range q.Items {
		if redisCluster.Name == rc.Name {
			q.Items = append(q.Items[:i], q.Items[i+1:]...)

			return true
		}
	}

	return false
}

// New create a new operator.
func New(clientset *client.Clientset, logger log.Logger) *Operator {
	o := &Operator{
		clientset:                 clientset,
		logger:                    logger,
		invalidRedisClusters:      queue{Name: "invalid"},
		unmarkedRedisClusters:     queue{Name: "unmarked"},
		initializingRedisClusters: queue{Name: "initializing"},
		initializedRedisClusters:  queue{Name: "initialized"},
	}

	source := cache.NewListWatchFromClient(
		o.clientset.RedisClustersClient,
		crd.RedisClusterDefinitionName,
		v1.NamespaceAll,
		fields.Everything(),
	)

	o.redisClustersStore, o.redisClustersController = cache.NewInformer(
		source,
		&crd.RedisCluster{},
		time.Minute,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    o.onAdd,
			UpdateFunc: o.onUpdate,
			DeleteFunc: o.onDelete,
		},
	)

	source = cache.NewListWatchFromClient(
		o.clientset.AppsV1beta1().RESTClient(),
		"statefulsets",
		v1.NamespaceAll,
		fields.Everything(),
	)

	o.statefulSetsStore, o.statefulSetsController = cache.NewInformer(
		source,
		&v1beta1.StatefulSet{},
		0,
		cache.ResourceEventHandlerFuncs{},
	)

	return o
}

// Run start the operator for as long as the specified context lives.
func (o *Operator) Run(ctx context.Context) {
	go o.statefulSetsController.Run(ctx.Done())
	o.redisClustersController.Run(ctx.Done())
}

func (o *Operator) getQueue(redisCluster *crd.RedisCluster) *queue {
	switch redisCluster.Status.State {
	case "":
		return &o.unmarkedRedisClusters
	case crd.RedisClusterStateInitializing:
		return &o.initializingRedisClusters
	case crd.RedisClusterStateInitialized:
		return &o.initializedRedisClusters
	default:
		return &o.invalidRedisClusters
	}
}

func (o *Operator) onAdd(new interface{}) {
	defer o.synchronize()

	o.lock.Lock()
	defer o.lock.Unlock()

	newRedisCluster := new.(*crd.RedisCluster)
	newQueue := o.getQueue(newRedisCluster)

	if newQueue.Add(newRedisCluster) {
		o.logger.Log("event", "added", "redis-cluster", newRedisCluster.Name, "queue", newQueue)
	}
}

func (o *Operator) onUpdate(old interface{}, new interface{}) {
	defer o.synchronize()

	o.lock.Lock()
	defer o.lock.Unlock()

	oldRedisCluster := old.(*crd.RedisCluster)
	oldQueue := o.getQueue(oldRedisCluster)
	newRedisCluster := new.(*crd.RedisCluster)
	newQueue := o.getQueue(newRedisCluster)

	if oldQueue != newQueue {
		removed := oldQueue.Remove(oldRedisCluster)
		added := newQueue.Add(newRedisCluster)

		if removed {
			if added {
				o.logger.Log("event", "updated", "redis-cluster", oldRedisCluster.Name, "old-queue", oldQueue, "new-queue", newQueue)
			} else {
				o.logger.Log("event", "removed", "redis-cluster", oldRedisCluster.Name, "old-queue", oldQueue)
			}
		} else if added {
			o.logger.Log("event", "added", "redis-cluster", newRedisCluster.Name, "queue", newQueue)
		}
	}
}

func (o *Operator) onDelete(old interface{}) {
	defer o.synchronize()

	o.lock.Lock()
	defer o.lock.Unlock()

	oldRedisCluster := old.(*crd.RedisCluster)
	oldQueue := o.getQueue(oldRedisCluster)

	if oldQueue.Remove(oldRedisCluster) {
		o.logger.Log("event", "removed", "redis-cluster", oldRedisCluster.Name, "queue", oldQueue)
	}
}

func (o *Operator) synchronize() {
	o.lock.Lock()
	defer o.lock.Unlock()

	var operations []Operation

	for _, redisCluster := range o.unmarkedRedisClusters.Items {
		operations = append(operations, SetStatus(o.clientset.RedisClustersClient, redisCluster, crd.RedisClusterStateInitializing, "Initializing Redis cluster."))
	}

	for _, redisCluster := range o.initializingRedisClusters.Items {
		expectedStatefulSet := o.getExpectedStatefulSetFor(redisCluster)

		if statefulSet := o.getStatefulSetFor(redisCluster); statefulSet != nil {
			if compareStatefulSets(statefulSet, expectedStatefulSet) {
				operations = append(operations, SetStatus(o.clientset.RedisClustersClient, redisCluster, crd.RedisClusterStateInitialized, "Setting-up shards in Redis cluster."))
			} else {
				// TODO: Update.
			}
		} else {
			operations = append(operations, CreateStatefulSet(o.clientset.AppsV1beta1().RESTClient(), expectedStatefulSet))
		}
	}

	for _, operation := range operations {
		if err := operation(); err != nil {
			o.logger.Log("event", "operation failure", "error", err)
		}
	}
}

func (o *Operator) getExpectedStatefulSetFor(redisCluster *crd.RedisCluster) *v1beta1.StatefulSet {
	replicas := int32(redisCluster.Spec.Instances * redisCluster.Spec.Duplicates)

	statefulSet := &v1beta1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      redisCluster.Name,
			Namespace: redisCluster.Namespace,
			Annotations: map[string]string{
				CreatedByAnnotation: fmt.Sprintf("%s", redisCluster.UID),
			},
		},
		Spec: v1beta1.StatefulSetSpec{
			ServiceName:          redisCluster.Name,
			Replicas:             &replicas,
			Template:             redisCluster.Spec.Template,
			VolumeClaimTemplates: redisCluster.Spec.VolumeClaimTemplates,
			PodManagementPolicy:  redisCluster.Spec.PodManagementPolicy,
			UpdateStrategy:       redisCluster.Spec.UpdateStrategy,
			RevisionHistoryLimit: redisCluster.Spec.RevisionHistoryLimit,
		},
	}

	return statefulSet
}

func (o *Operator) getStatefulSetFor(redisCluster *crd.RedisCluster) *v1beta1.StatefulSet {
	for _, statefulSet := range o.statefulSetsStore.List() {
		statefulSet := statefulSet.(*v1beta1.StatefulSet)

		if statefulSet.Namespace != redisCluster.Namespace {
			continue
		}

		if statefulSet.Name != redisCluster.Name {
			continue
		}

		return statefulSet
	}

	return nil
}

func compareStatefulSets(lhs, rhs *v1beta1.StatefulSet) bool {
	lb, _ := lhs.Spec.Marshal()
	rb, _ := rhs.Spec.Marshal()

	if bytes.Compare(lb, rb) != 0 {
		return false
	}

	return true
}
