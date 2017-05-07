package k8s

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// GetConfiguration returns the Kubernetes configuration. If a path is
// specified, the configuration file must exist. Otherwise, auto-cluster
// detection applies. If the auto-cluster detection fails, the default
// configuration file is tried as a last resort.
func GetConfiguration(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	config, lastErr := rest.InClusterConfig()

	if lastErr != nil {
		var err error
		config, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)

		if err == nil {
			return config, nil
		}
	}

	return config, lastErr
}
