package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/ereOn/k8s-redis-cluster-operator/k8s"
	"github.com/go-kit/kit/log"
	"github.com/spf13/cobra"
)

var (
	kubeconfig string
)

func init() {
	RootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", "", "The path to a kubectl configuration. Necessary when the tool runs outside the cluster.")
}

func waitInterrupt() {
	terminated := make(chan os.Signal, 1)
	defer close(terminated)
	signal.Notify(terminated, os.Interrupt)
	<-terminated
}

var RootCmd = &cobra.Command{
	Use:   "redis-cluster-operator",
	Short: "An operator that operates Redis clusters.",
	Long:  "An operator that operates Redis clusters.",
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := k8s.GetConfiguration(kubeconfig)

		if err != nil {
			return err
		}

		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		clientset, err := k8s.NewForConfig(logger, config)

		if err != nil {
			return err
		}

		err = clientset.RegisterThirdPartyResources()

		if err != nil {
			return err
		}

		defer clientset.Close()
		defer logger.Log("event", "watch ended")

		logger.Log("event", "watch started")
		go clientset.Watch()

		waitInterrupt()

		return nil
	},
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
