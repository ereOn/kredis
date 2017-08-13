package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-kit/kit/log"
	"github.com/spf13/cobra"
)

// WithCancelOnInterupt returns a context that expires when an interruption occurs.
func WithCancelOnInterupt(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer cancel()

		terminated := make(chan os.Signal, 1)
		defer close(terminated)

		signal.Notify(terminated, os.Interrupt, syscall.SIGTERM)
		defer signal.Reset(os.Interrupt, syscall.SIGTERM)

		<-terminated
	}()

	return ctx, cancel
}

var rootCmd = &cobra.Command{
	Use:   "kredis <master-group>...",
	Short: "A tool to manage Redis clusters in Kubernetes.",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := WithCancelOnInterupt(context.Background())
		defer cancel()

		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

		logger.Log("event", "started")
		defer logger.Log("event", "stopped")

		<-ctx.Done()

		return nil
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
