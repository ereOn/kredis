package k8s

import (
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
)

// K8SRedisClusterResourceName is the "Redis Cluster" resource name.
var K8SRedisClusterResourceName = "k8s-redis-cluster.freelan.org"

// RegisterThirdPartyResource makes sure the third-party resource is registered.
func RegisterThirdPartyResource(clientset *kubernetes.Clientset) (*v1beta1.ThirdPartyResource, error) {
	tpr, err := GetThirdPartyResource(clientset)

	if err != nil {
		return nil, err
	}

	if tpr != nil {
		return tpr, nil
	}

	return CreateThirdPartyResource(clientset)
}

// UnregisterThirdPartyResource makes sure the third-party resource is unregistered.
func UnregisterThirdPartyResource(clientset *kubernetes.Clientset) error {
	tpr, err := GetThirdPartyResource(clientset)

	if err != nil {
		return err
	}

	if tpr != nil {
		return DeleteThirdPartyResource(clientset)
	}

	return nil
}

// GetThirdPartyResource returns the third-party resource if it was registered
// or nil otherwise.
func GetThirdPartyResource(clientset *kubernetes.Clientset) (*v1beta1.ThirdPartyResource, error) {
	tpr, err := clientset.ExtensionsV1beta1().ThirdPartyResources().Get(K8SRedisClusterResourceName, metav1.GetOptions{})

	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	return tpr, nil
}

// CreateThirdPartyResource creates the third-party resource.
func CreateThirdPartyResource(clientset *kubernetes.Clientset) (*v1beta1.ThirdPartyResource, error) {
	tpr := &v1beta1.ThirdPartyResource{
		ObjectMeta: metav1.ObjectMeta{
			Name: K8SRedisClusterResourceName,
		},
		Versions: []v1beta1.APIVersion{
			{Name: "v1"},
		},
		Description: "A Redis Cluster",
	}

	return clientset.ExtensionsV1beta1().ThirdPartyResources().Create(tpr)
}

// DeleteThirdPartyResource deletes the third-party resource.
func DeleteThirdPartyResource(clientset *kubernetes.Clientset) error {
	return clientset.ExtensionsV1beta1().ThirdPartyResources().Delete(K8SRedisClusterResourceName, &metav1.DeleteOptions{})
}
