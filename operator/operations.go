package operator

import (
	"encoding/json"
	"fmt"

	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	"k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// SetServiceInfo set the status of a Redis cluster.
func SetServiceInfo(client *rest.RESTClient, redisCluster *crd.RedisCluster, uid types.UID) error {
	body, _ := json.Marshal(struct {
		metav1.ObjectMeta `json:"metadata"`
	}{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				ServiceAnnotation: string(uid),
			},
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
		return fmt.Errorf("setting service info on Redis cluster \"%s\" in namespace \"%s\": %s", redisCluster.Name, redisCluster.Namespace, err)
	}

	return nil
}

// SetStatefulSetInfo set the status of a Redis cluster.
func SetStatefulSetInfo(client *rest.RESTClient, redisCluster *crd.RedisCluster, uid types.UID) error {
	body, _ := json.Marshal(struct {
		metav1.ObjectMeta `json:"metadata"`
	}{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				StatefulSetAnnotation: string(uid),
			},
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
		return fmt.Errorf("setting stateful-set info on Redis cluster \"%s\" in namespace \"%s\": %s", redisCluster.Name, redisCluster.Namespace, err)
	}

	return nil
}

// CreateStatefulSet create a new stateful-set.
func CreateStatefulSet(client rest.Interface, statefulSet *v1beta1.StatefulSet) (*v1beta1.StatefulSet, error) {
	var result v1beta1.StatefulSet

	err := client.Post().
		Resource("statefulsets").
		Namespace(statefulSet.Namespace).
		Body(statefulSet).
		Do().
		Into(&result)

	if err != nil {
		return nil, fmt.Errorf("creating statefulset \"%s\" in namespace \"%s\": %s", statefulSet.Name, statefulSet.Namespace, err)
	}

	return &result, nil
}

// CreateService create a new service.
func CreateService(client rest.Interface, service *v1.Service) (*v1.Service, error) {
	var result v1.Service

	err := client.Post().
		Resource("services").
		Namespace(service.Namespace).
		Body(service).
		Do().
		Into(&result)

	if err != nil {
		return nil, fmt.Errorf("creating service \"%s\" in namespace \"%s\": %s", service.Name, service.Namespace, err)
	}

	return &result, nil
}

// SetStatefulSetReplicas set the replicas of a stateful-set.
func SetStatefulSetReplicas(client rest.Interface, statefulSet *v1beta1.StatefulSet, replicas int32) error {
	var patch struct {
		Spec struct {
			Replicas *int32 `json:"replicas"`
		} `json:"spec"`
	}
	patch.Spec.Replicas = &replicas

	body, _ := json.Marshal(patch)

	err := client.
		Patch(types.MergePatchType).
		Resource("statefulsets").
		Namespace(statefulSet.Namespace).
		Name(statefulSet.Name).
		Body(body).
		Do().
		Error()

	if err != nil {
		return fmt.Errorf("setting stateful-set replicas on StatefulSet \"%s\" in namespace \"%s\": %s", statefulSet.Name, statefulSet.Namespace, err)
	}

	return nil
}
