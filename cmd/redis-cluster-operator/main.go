package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/ereOn/k8s-redis-cluster-operator/tpr"
	"github.com/spf13/cobra"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/errors"
	"k8s.io/client-go/pkg/api/unversioned"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/pkg/runtime"
	"k8s.io/client-go/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	kubeconfig string
	config     *rest.Config
)

func init() {
	RootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", "", "The path to a kubectl configuration. Necessary when the tool runs outside the cluster.")
}

var RootCmd = &cobra.Command{
	Use:   "redis-cluster-operator",
	Short: "An operator that operates Redis clusters.",
	Long:  "An operator that operates Redis clusters.",
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := configFromFlags(kubeconfig)

		if err != nil {
			return err
		}

		//tprclient, err := buildClientFromFlags(config)

		//if err != nil {
		//	return err
		//}

		clientset, err := kubernetes.NewForConfig(config)

		if err != nil {
			return err
		}

		err = initializeResources(clientset)

		if err != nil {
			return err
		}

		terminated := make(chan os.Signal, 1)
		defer close(terminated)
		signal.Notify(terminated, os.Interrupt)
		<-terminated

		return nil
	},
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func buildClientFromFlags(config *rest.Config) (*rest.RESTClient, error) {
	config.GroupVersion = &unversioned.GroupVersion{
		Group:   "freelan.org",
		Version: "v1",
	}
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: api.Codecs}

	schemeBuilder := runtime.NewSchemeBuilder(addKnownTypes)
	schemeBuilder.AddToScheme(api.Scheme)

	return rest.RESTClientFor(config)
}

func configFromFlags(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	return rest.InClusterConfig()
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(
		unversioned.GroupVersion{Group: "freelan.org", Version: "v1"},
		&tpr.RedisCluster{},
		&tpr.RedisClusterList{},
	)

	return nil
}

func initializeResources(clientset *kubernetes.Clientset) error {
	// initialize third party resources if they do not exist
	_, err := clientset.Extensions().ThirdPartyResources().Get("redis-cluster.freelan.org")

	if err != nil {
		if errors.IsNotFound(err) {
			tpr := &v1beta1.ThirdPartyResource{
				ObjectMeta: v1.ObjectMeta{
					Name: "redis-cluster.freelan.org",
				},
				Versions: []v1beta1.APIVersion{
					{Name: "v1"},
				},
				Description: "A Redis cluster third-party resource.",
			}

			_, err := clientset.Extensions().ThirdPartyResources().Create(tpr)

			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}
