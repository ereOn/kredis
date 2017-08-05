package main

import (
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	crv1 "k8s.io/apiextensions-apiserver/examples/client-go/apis/cr/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"

	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-kit/kit/log"
	"github.com/spf13/cobra"
)

var (
	kubeconfig = clientcmd.RecommendedHomeFile
)

func waitInterrupt() {
	terminated := make(chan os.Signal, 1)
	defer close(terminated)
	signal.Notify(terminated, os.Interrupt, syscall.SIGTERM)
	<-terminated
}

func CreateCustomResourceDefinition(clientset apiextensionsclient.Interface) (*apiextensionsv1beta1.CustomResourceDefinition, error) {
	crd := &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: crv1.ExampleResourcePlural + "." + crv1.GroupName,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   crv1.GroupName,
			Version: crv1.SchemeGroupVersion.Version,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural: crv1.ExampleResourcePlural,
				Kind:   reflect.TypeOf(crv1.Example{}).Name(),
			},
		},
	}

	return clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
}

var rootCmd = &cobra.Command{
	Use:   "rco",
	Short: "An operator that operates Redis clusters.",
	Long:  "An operator that operates Redis clusters.",
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := buildConfig(kubeconfig)

		if err != nil {
			return err
		}

		apiextensionsclientset, err := apiextensionsclient.NewForConfig(config)

		if err != nil {
			panic(err)
		}

		fmt.Println(CreateCustomResourceDefinition(apiextensionsclientset))

		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		logger.Log("event", "watch started")
		defer logger.Log("event", "watch ended")

		waitInterrupt()

		return nil
	},
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)

	if err != nil {
		config, err = rest.InClusterConfig()
	}

	return config, err
}

func init() {
	rootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", kubeconfig, "The path to a kubectl configuration. Necessary when the tool runs outside the cluster.")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
