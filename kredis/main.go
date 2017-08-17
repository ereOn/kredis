package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ereOn/kredis/pkg/kredis"
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
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		masterGroups := make([]kredis.MasterGroup, len(args))

		for i, arg := range args {
			masterGroups[i], err = kredis.ParseMasterGroup(arg)

			if err != nil {
				return fmt.Errorf("parsing argument %d: %s", i, err)
			}
		}

		if len(masterGroups) == 0 {
			return errors.New("no master groups specified - refusing to run")
		}

		cmd.SilenceUsage = true

		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

		logger.Log("event", "master groups", "count", len(masterGroups))

		for i, masterGroup := range masterGroups {
			logger.Log("event", "master group", "index", i, "master-group", masterGroup)
		}

		pool := &kredis.Pool{
			IdleTimeout: time.Second * 90,
			MaxActive:   10,
			MaxIdle:     2,
		}
		defer pool.Close()

		manager := &kredis.Manager{
			Logger:     logger,
			Pool:       pool,
			SyncPeriod: time.Second,
		}

		logger.Log("event", "started")
		defer logger.Log("event", "stopped")

		ctx, cancel := WithCancelOnInterupt(context.Background())
		defer cancel()

		manager.Run(ctx, masterGroups)
		return nil
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
