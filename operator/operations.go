package operator

import (
	"encoding/json"
	"fmt"

	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	"k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// SetStatus set the status of a Redis cluster.
func SetStatus(client *rest.RESTClient, redisCluster *crd.RedisCluster, state crd.RedisClusterState, message string) error {
	body, _ := json.Marshal(struct {
		Status crd.RedisClusterStatus `json:"status"`
	}{
		Status: crd.RedisClusterStatus{
			State:   state,
			Message: message,
		},
	})

	err := client.
		Patch(types.MergePatchType).
		Resource(crd.RedisClusterDefinitionName).
		Namespace(redisCluster.Namespace).
		Name(redisCluster.Name).
		Body(body).
		Do().
		Error()

	if err != nil {
		return fmt.Errorf("setting status on Redis cluster \"%s\" in namespace \"%s\": %s", redisCluster.Name, redisCluster.Namespace, err)
	}

	return nil
}

// CreateStatefulSet create a new stateful set.
func CreateStatefulSet(client rest.Interface, statefulSet *v1beta1.StatefulSet) error {
	err := client.Post().
		Resource("statefulsets").
		Namespace(statefulSet.Namespace).
		Body(statefulSet).
		Do().
		Error()

	if err != nil {
		return fmt.Errorf("creating statefulset \"%s\" in namespace \"%s\": %s", statefulSet.Name, statefulSet.Namespace, err)
	}

	return nil
}

// CreateService create a new service.
func CreateService(client rest.Interface, service *v1.Service) error {
	err := client.Post().
		Resource("services").
		Namespace(service.Namespace).
		Body(service).
		Do().
		Error()

	if err != nil {
		return fmt.Errorf("creating service \"%s\" in namespace \"%s\": %s", service.Name, service.Namespace, err)
	}

	return nil
}
