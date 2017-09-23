package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ereOn/kredis/pkg/kredis"
	"github.com/garyburd/redigo/redis"
	"github.com/go-kit/kit/log"
	"github.com/mna/redisc"
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

var maxClusterRefreshDuration = time.Second * 10
var countersCount = 32768

func createPool(addr string, opts ...redis.DialOption) (*redis.Pool, error) {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr, opts...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxActive:   100,
		MaxIdle:     10,
		IdleTimeout: 30 * time.Second,
	}, nil
}

var rootCmd = &cobra.Command{
	Use:   "kredis-test redis-host...",
	Short: "A tool to test kredis clusters.",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		if len(args) != 1 {
			return errors.New("unexpected number of arguments")
		}

		redisHosts := args

		cmd.SilenceUsage = true

		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

		logger.Log("event", "test starting", "redis-hosts", strings.Join(redisHosts, ","))
		defer logger.Log("event", "test stopping")

		ctx, cancel := WithCancelOnInterupt(context.Background())
		defer cancel()

		cluster := &redisc.Cluster{
			StartupNodes: redisHosts,
			DialOptions:  []redis.DialOption{redis.DialConnectTimeout(5 * time.Second)},
			CreatePool:   createPool,
		}

		errorFeed := kredis.ErrorFeed{Threshold: maxClusterRefreshDuration}

		logger.Log("event", "refreshing cluster in progress", "maximum-duration", maxClusterRefreshDuration)

		for err := cluster.Refresh(); err != nil; err = cluster.Refresh() {
			errorFeed.Add(err)

			if items := errorFeed.PopErrors(); len(items) != 0 {
				for _, item := range items {
					logger.Log("event", "cluster refresh failure", "error", item.Error, "count", item.Count)
				}

				return errors.New("cluster refresh failed")
			}
		}

		logger.Log("event", "cluster refresh complete")
		logger.Log("event", "starting consistency test")

		ticker := time.NewTicker(time.Millisecond * 1)
		defer ticker.Stop()

		errorFeed.Reset()

		for {
			select {
			case <-ctx.Done():
				return cluster.Close()
			case <-ticker.C:
				if err := incrTest(cluster); err != nil {
					errorFeed.Add(err)
				}

				if items := errorFeed.PopErrors(); len(items) != 0 {
					for _, item := range items {
						logger.Log("event", "consistency failure", "error", item.Error, "count", item.Count)
					}
				}
			}
		}
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

var counters = map[string]int{}

func incrTest(cluster *redisc.Cluster) error {
	conn := cluster.Get()
	conn, _ = redisc.RetryConn(conn, 5, time.Millisecond*5)
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return err
	}

	key := strconv.Itoa(rand.Intn(countersCount))

	currentValue, ok := counters[key]
	expectedValue := currentValue + 1
	value, err := redis.Int(conn.Do("INCR", key))

	if err != nil {
		return err
	}

	counters[key] = value

	if ok && value != expectedValue {
		return fmt.Errorf("expected counter \"%s\" to be %d but was %d", key, expectedValue, value)
	}

	return nil
}
