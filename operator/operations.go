package operator

import (
	"encoding/json"
	"fmt"

	"github.com/ereOn/k8s-redis-cluster-operator/crd"
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
			return fmt.Errorf("failed to patch Redis cluster \"%s\": %s", redisCluster.Name, err)
		}

		return nil
	}
}
