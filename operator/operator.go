package operator

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ereOn/k8s-redis-cluster-operator/client"
	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	"github.com/go-kit/kit/log"

	"k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	redisClustersStore        cache.Store
	redisClustersController   cache.Controller
	replicaSetsStore          cache.Store
	replicaSetsController     cache.Controller
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
		o.clientset.ExtensionsV1beta1().RESTClient(),
		"replicasets",
		v1.NamespaceAll,
		fields.Everything(),
	)

	o.replicaSetsStore, o.replicaSetsController = cache.NewInformer(
		source,
		&v1beta1.ReplicaSet{},
		0,
		cache.ResourceEventHandlerFuncs{},
	)

	return o
}

// Run start the operator for as long as the specified context lives.
func (o *Operator) Run(ctx context.Context) {
	go o.replicaSetsController.Run(ctx.Done())
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
		operations = append(operations, SetStatus(o.clientset.RedisClustersClient, redisCluster, crd.RedisClusterStateInitializing, "Initializing Redis cluster"))
	}

	for _, redisCluster := range o.initializingRedisClusters.Items {
		expectedReplicaSets := o.getExpectedReplicaSetsFor(redisCluster)
		replicaSets, extraReplicaSets := o.getReplicaSetsFor(redisCluster)

		for i, replicaSet := range replicaSets {
			if replicaSet == nil {
				operations = append(operations, CreateReplicaSet(o.clientset.ExtensionsV1beta1().RESTClient(), expectedReplicaSets[i]))
			} else {
				if !compareReplicaSets(expectedReplicaSets[i], replicaSet) {
					operations = append(operations, UpdateReplicaSet(o.clientset.ExtensionsV1beta1().RESTClient(), expectedReplicaSets[i]))
				}
			}
		}

		for _, replicaSet := range extraReplicaSets {
			operations = append(operations, DeleteReplicaSet(o.clientset.ExtensionsV1beta1().RESTClient(), replicaSet.Namespace, replicaSet.Name))
		}
	}

	for _, operation := range operations {
		if err := operation(); err != nil {
			o.logger.Log("event", "operation failure", "error", err)
		}
	}
}

func (o *Operator) getExpectedReplicaSetsFor(redisCluster *crd.RedisCluster) (result []*v1beta1.ReplicaSet) {
	var replicas int32 = 1

	for i := 0; i < redisCluster.Spec.Instances; i++ {
		for j := 0; j < redisCluster.Spec.Duplicates; j++ {
			name := fmt.Sprintf("%s-%d-%d", redisCluster.Name, i, j)

			replicaSet := &v1beta1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: redisCluster.Namespace,
				},
				Spec: v1beta1.ReplicaSetSpec{
					Replicas: &replicas,
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name: name,
							Labels: map[string]string{
								"app": "redis",
							},
							Annotations: map[string]string{
								CreatedByAnnotation:      fmt.Sprintf("%s", redisCluster.UID),
								PrimaryIndexAnnotation:   fmt.Sprintf("%d", i),
								SecondaryIndexAnnotation: fmt.Sprintf("%d", j),
							},
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								v1.Container{
									Name:    "redis",
									Image:   "redis:3.2.0-alpine",
									Command: []string{"redis-server"},
									Args: []string{
										"/etc/redis/redis.conf",
										"--protected-mode",
										"no",
									},
									Resources: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("100m"),
											v1.ResourceMemory: resource.MustParse("100Mi"),
										},
									},
									Ports: []v1.ContainerPort{
										v1.ContainerPort{
											Name:          "redis",
											ContainerPort: 6379,
											Protocol:      v1.ProtocolTCP,
										},
										v1.ContainerPort{
											Name:          "redis-cluster",
											ContainerPort: 16379,
											Protocol:      v1.ProtocolTCP,
										},
									},
								},
							},
						},
					},
				},
			}

			result = append(result, replicaSet)
		}
	}

	return
}

func (o *Operator) getReplicaSetsFor(redisCluster *crd.RedisCluster) (owned []*v1beta1.ReplicaSet, extra []*v1beta1.ReplicaSet) {
	owned = make([]*v1beta1.ReplicaSet, redisCluster.Spec.Instances)

	redisClusterUID := fmt.Sprintf("%s", redisCluster.UID)

	for _, replicaSet := range o.replicaSetsStore.List() {
		replicaSet := replicaSet.(*v1beta1.ReplicaSet)

		if replicaSet.Namespace != redisCluster.Namespace {
			continue
		}

		if uid, ok := replicaSet.GetAnnotations()[CreatedByAnnotation]; !ok || uid != redisClusterUID {
			continue
		}

		primaryIndex, err := strconv.Atoi(replicaSet.GetAnnotations()[PrimaryIndexAnnotation])

		if err != nil {
			extra = append(extra, replicaSet)
			continue
		}

		secondaryIndex, err := strconv.Atoi(replicaSet.GetAnnotations()[SecondaryIndexAnnotation])

		if err != nil {
			extra = append(extra, replicaSet)
			continue
		}

		index := primaryIndex*redisCluster.Spec.Instances + secondaryIndex

		if owned[index] != nil {
			extra = append(extra, replicaSet)
			continue
		}

		owned[index] = replicaSet
	}

	return
}

func compareReplicaSets(lhs, rhs *v1beta1.ReplicaSet) bool {
	lb, _ := lhs.Spec.Marshal()
	rb, _ := rhs.Spec.Marshal()

	if bytes.Compare(lb, rb) != 0 {
		return false
	}

	return true
}
