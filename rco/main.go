package main

import (
	"os"
	"os/signal"
	"syscall"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"

	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/ereOn/k8s-redis-cluster-operator/crd"
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

func buildConfig(kubeconfig string) (*rest.Config, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)

	if err != nil {
		config, err = rest.InClusterConfig()
	}

	return config, err
}

var clientset *apiextensionsclient.Clientset

var rootCmd = &cobra.Command{
	Use:   "rco",
	Short: "An operator that manages Redis clusters.",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		config, err := buildConfig(kubeconfig)

		if err != nil {
			return err
		}

		clientset, err = apiextensionsclient.NewForConfig(config)

		if err != nil {
			return err
		}

		return nil
	},
}

var registerCmd = &cobra.Command{
	Use:          "register",
	Short:        "Register the custom resource definition in the Kubernetes cluster.",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		_, err := crd.Register(clientset)

		return err
	},
}

var unregisterCmd = &cobra.Command{
	Use:          "unregister",
	Short:        "Unregister the custom resource definition from the Kubernetes cluster.",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return crd.Unregister(clientset)
	},
}

var manageCmd = &cobra.Command{
	Use:          "manager",
	Short:        "Manage the Redis cluster resource definitions in the Kubernetes cluster.",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		logger.Log("event", "watch started")
		defer logger.Log("event", "watch ended")

		waitInterrupt()

		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", kubeconfig, "The path to a kubectl configuration. Necessary when the tool runs outside the cluster.")

	rootCmd.AddCommand(registerCmd)
	rootCmd.AddCommand(unregisterCmd)
	rootCmd.AddCommand(manageCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
