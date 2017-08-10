package operator

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/ereOn/k8s-redis-cluster-operator/client"
	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	"github.com/go-kit/kit/log"

	"k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/cache"
)

// An Operator manages Redis cluster resources in a Kubernetes Cluster.
//
// There should be only one operator running at any given time in a kubernetes
// cluster.
type Operator struct {
	clientset               *client.Clientset
	logger                  log.Logger
	redisClustersStore      cache.Store
	redisClustersController cache.Controller
	servicesStore           cache.Store
	servicesController      cache.Controller
	statefulSetsStore       cache.Store
	statefulSetsController  cache.Controller
}

// New create a new operator.
func New(clientset *client.Clientset, logger log.Logger) *Operator {
	o := &Operator{
		clientset: clientset,
		logger:    logger,
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
		cache.ResourceEventHandlerFuncs{},
	)

	source = cache.NewListWatchFromClient(
		o.clientset.CoreV1().RESTClient(),
		"services",
		v1.NamespaceAll,
		fields.Everything(),
	)

	o.servicesStore, o.servicesController = cache.NewInformer(
		source,
		&v1.Service{},
		0,
		cache.ResourceEventHandlerFuncs{},
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
	go o.servicesController.Run(ctx.Done())
	go o.statefulSetsController.Run(ctx.Done())
	go o.redisClustersController.Run(ctx.Done())

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.synchronize()
		}
	}
}

func (o *Operator) synchronize() {
	for _, redisCluster := range o.redisClustersStore.List() {
		redisCluster := redisCluster.(*crd.RedisCluster)

		service, updated := o.getServiceFor(redisCluster)

		if service != nil {
			if updated {
				o.logger.Log("event", "services differ", "redis-cluster", redisCluster.Name, "namespace", redisCluster.Namespace)
				//expectedService := o.getExpectedServiceFor(redisCluster)

				continue
			}
		} else {
			expectedService := o.getExpectedServiceFor(redisCluster)

			service, err := CreateService(o.clientset.CoreV1().RESTClient(), expectedService)

			if err != nil {
				o.logger.Log("event", "failed to create service", "redis-cluster", redisCluster.Name, "namespace", redisCluster.Namespace, "error", err)
				// TODO: Write an event.
			} else {
				o.logger.Log("event", "created service", "redis-cluster", redisCluster.Name, "namespace", redisCluster.Namespace, "uid", service.UID)

				if err = SetServiceInfo(o.clientset.RedisClustersClient, redisCluster, service.UID, service.ResourceVersion); err != nil {
					o.logger.Log("event", "failed to update service info", "redis-cluster", redisCluster.Name, "namespace", redisCluster.Namespace, "error", err)
				}
			}

			continue
		}

		expectedStatefulSet := o.getExpectedStatefulSetFor(redisCluster)

		if statefulSet := o.getStatefulSetFor(redisCluster); statefulSet != nil {
			if !compareStatefulSets(expectedStatefulSet, statefulSet) {
				// TODO: Update the statefulSet and write an event.
				o.logger.Log("event", "stateful-sets differ", "redis-cluster", redisCluster.Name, "namespace", redisCluster.Namespace)
				continue
			}
		} else {
			if err := CreateStatefulSet(o.clientset.AppsV1beta1().RESTClient(), expectedStatefulSet); err != nil {
				o.logger.Log("event", "error", "redis-cluster", redisCluster.Name, "namespace", redisCluster.Namespace, "error", err)
				// TODO: Write an event.
			}

			continue
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

func compareStatefulSets(ref, actual *v1beta1.StatefulSet) bool {
	return actual.ObjectMeta.Name == ref.ObjectMeta.Name &&
		actual.ObjectMeta.Namespace == ref.ObjectMeta.Namespace &&
		reflect.DeepEqual(actual.ObjectMeta.Annotations, ref.ObjectMeta.Annotations) &&
		actual.Spec.ServiceName == ref.Spec.ServiceName &&
		*actual.Spec.Replicas == *ref.Spec.Replicas &&
		reflect.DeepEqual(actual.Spec.VolumeClaimTemplates, ref.Spec.VolumeClaimTemplates)
}

func (o *Operator) getExpectedServiceFor(redisCluster *crd.RedisCluster) *v1.Service {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      redisCluster.Name,
			Namespace: redisCluster.Namespace,
			Annotations: map[string]string{
				CreatedByAnnotation: fmt.Sprintf("%s", redisCluster.UID),
			},
			Labels: redisCluster.Spec.Template.ObjectMeta.Labels,
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				v1.ServicePort{
					Name:       "redis",
					Port:       6379,
					TargetPort: intstr.FromInt(6379),
					Protocol:   v1.ProtocolTCP,
				},
				v1.ServicePort{
					Name:       "redis-cluster",
					Port:       16379,
					TargetPort: intstr.FromInt(16379),
					Protocol:   v1.ProtocolTCP,
				},
			},
			ClusterIP:       v1.ClusterIPNone,
			Type:            v1.ServiceTypeClusterIP,
			SessionAffinity: v1.ServiceAffinityNone,
		},
	}

	return service
}

func getServiceUIDFor(redisCluster *crd.RedisCluster) types.UID {
	if x, ok := redisCluster.Annotations[ServiceAnnotation]; ok {
		return types.UID(x)
	}

	return ""
}

func getServiceVersionFor(redisCluster *crd.RedisCluster) string {
	version, _ := redisCluster.Annotations[ServiceVersionAnnotation]
	return version
}

func (o *Operator) getServiceFor(redisCluster *crd.RedisCluster) (*v1.Service, bool) {
	uid := getServiceUIDFor(redisCluster)

	if uid != "" {
		version := getServiceVersionFor(redisCluster)

		for _, service := range o.servicesStore.List() {
			service := service.(*v1.Service)

			if service.UID == uid {
				return service, service.ResourceVersion != version
			}
		}
	}

	return nil, false
}
