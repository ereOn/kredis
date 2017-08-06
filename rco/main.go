package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/ereOn/k8s-redis-cluster-operator/client"
	"github.com/ereOn/k8s-redis-cluster-operator/crd"
	"github.com/go-kit/kit/log"
	"github.com/spf13/cobra"
)

var (
	kubeconfig = clientcmd.RecommendedHomeFile
	namespace  = "default"
	instances  = 3
	slaves     = 1
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
var redisClusterClient *rest.RESTClient
var redisClusterScheme *runtime.Scheme

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

		redisClusterClient, redisClusterScheme, err = client.New(config)

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

var createCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new Redis cluster in the Kubernetes cluster.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("you must specify a `name` for the Redis cluster")
		}

		cmd.SilenceUsage = true

		name := args[0]
		redisCluster := crd.RedisCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: crd.RedisClusterSpec{
				Instances: instances,
				Slaves:    slaves,
			},
		}

		if err := redisClusterClient.Post().Resource(crd.RedisClusterDefinitionName).Namespace(namespace).Body(&redisCluster).Do().Into(&redisCluster); err != nil {
			return fmt.Errorf("failed to create Redis cluster: %s", err)
		}

		fmt.Printf("%s \"%s\" created\n", crd.RedisClusterDefinitionNameSingular, name)

		return nil
	},
}

var deleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a Redis cluster from the Kubernetes cluster.",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("you must specify a `name` for the Redis cluster")
		}

		cmd.SilenceUsage = true

		name := args[0]

		if err := redisClusterClient.Delete().Resource(crd.RedisClusterDefinitionName).Namespace(namespace).Do().Error(); err != nil {
			return fmt.Errorf("failed to delete Redis cluster: %s", err)
		}

		fmt.Printf("%s \"%s\" deleted\n", crd.RedisClusterDefinitionNameSingular, name)

		return nil
	},
}

var manageCmd = &cobra.Command{
	Use:          "manage",
	Short:        "Manage the Redis cluster resource definitions in the Kubernetes cluster.",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		redisClusters := crd.RedisClusterList{}
		err := redisClusterClient.Get().Resource(crd.RedisClusterDefinitionName).Do().Into(&redisClusters)

		if err != nil {
			return err
		}

		for _, redisCluster := range redisClusters.Items {
			fmt.Println(redisCluster.Name, redisCluster.Spec.Instances, redisCluster.Spec.Slaves)
		}

		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		logger.Log("event", "watch started")
		defer logger.Log("event", "watch ended")

		waitInterrupt()

		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", kubeconfig, "The path to a kubectl configuration. Necessary when the tool runs outside the cluster.")
	createCmd.Flags().StringVarP(&namespace, "namespace", "n", namespace, "The namespace to create the Redis cluster into.")
	createCmd.Flags().IntVar(&instances, "instances", instances, "The number of master Redis instances.")
	createCmd.Flags().IntVar(&slaves, "slaves", slaves, "The number of slave Redis instances per master.")
	deleteCmd.Flags().StringVarP(&namespace, "namespace", "n", namespace, "The namespace to delete the Redis cluster from.")

	rootCmd.AddCommand(registerCmd)
	rootCmd.AddCommand(unregisterCmd)
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(manageCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
