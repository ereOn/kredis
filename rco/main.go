package main

import (
	"os"
	"os/signal"
	"syscall"

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
		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		logger.Log("event", "watch started")
		defer logger.Log("event", "watch ended")

		waitInterrupt()

		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", "", "The path to a kubectl configuration. Necessary when the tool runs outside the cluster.")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
