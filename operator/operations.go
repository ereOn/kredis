package operator

import (
	"encoding/json"
	"fmt"

	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	"k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// An Operation performed synchronously.
type Operation func() error

// SetStatus set the status of a Redis cluster.
func SetStatus(client *rest.RESTClient, redisCluster *crd.RedisCluster, state crd.RedisClusterState, message string) Operation {
	body, _ := json.Marshal(struct {
		Status crd.RedisClusterStatus `json:"status"`
	}{
		Status: crd.RedisClusterStatus{
			State:   state,
			Message: message,
		},
	})

	return func() error {
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
}

// CreateReplicaSet create a new replicaset.
func CreateReplicaSet(client rest.Interface, replicaSet *v1beta1.ReplicaSet) Operation {
	return func() error {
		err := client.Post().
			Resource("replicasets").
			Namespace(replicaSet.Namespace).
			Body(replicaSet).
			Do().
			Error()

		if err != nil {
			return fmt.Errorf("creating replicaset \"%s\" in namespace \"%s\": %s", replicaSet.Name, replicaSet.Namespace, err)
		}

		return nil
	}
}

// DeleteReplicaSet delete a new replicaset.
func DeleteReplicaSet(client rest.Interface, namespace string, name string) Operation {
	return func() error {
		err := client.Delete().
			Resource("replicasets").
			Namespace(namespace).
			Name(name).
			Do().
			Error()

		if err != nil {
			return fmt.Errorf("deleting replicaset \"%s\" in namespace \"%s\": %s", name, namespace, err)
		}

		return nil
	}
}

// UpdateReplicaSet update a replicaset.
func UpdateReplicaSet(client rest.Interface, replicaSet *v1beta1.ReplicaSet) Operation {
	return func() error {
		err := client.Put().
			Resource("replicasets").
			Namespace(replicaSet.Namespace).
			Name(replicaSet.Name).
			Body(replicaSet).
			Do().
			Error()

		if err != nil {
			return fmt.Errorf("updating replicaset \"%s\" in namespace \"%s\": %s", replicaSet.Name, replicaSet.Namespace, err)
		}

		return nil
	}
}
