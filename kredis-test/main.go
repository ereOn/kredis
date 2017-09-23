package main

import (
	"context"
	"errors"
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
	Use:   "kredis-test redis-host",
	Short: "A tool to test kredis clusters.",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		if len(args) != 1 {
			return errors.New("unexpected number of arguments")
		}

		redisHost := args[0]

		cmd.SilenceUsage = true

		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

		logger.Log("event", "test starting", "redis-host", redisHost)
		defer logger.Log("event", "test stopping")

		ctx, cancel := WithCancelOnInterupt(context.Background())
		defer cancel()

		<-ctx.Done()

		return nil
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
