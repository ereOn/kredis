package main

import (
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/go-kit/kit/log"
	"github.com/spf13/cobra"
)

var (
	kubeconfig string
)

func waitInterrupt() {
	terminated := make(chan os.Signal, 1)
	defer close(terminated)
	signal.Notify(terminated, os.Interrupt, syscall.SIGTERM)
	<-terminated
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

		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		logger.Log("event", "watch started")
		defer logger.Log("event", "watch ended")

		waitInterrupt()

		return nil
	},
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", "", "The path to a kubectl configuration. Necessary when the tool runs outside the cluster.")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
