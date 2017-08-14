package kredis

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
)

// A Manager is responsible for the coordination of Redis instances inside a
// MasterGroup.
type Manager struct {
	logger log.Logger
	pool   *Pool
}

// NewManager instantiates a new Manager.
func NewManager(logger log.Logger, pool *Pool) *Manager {
	return &Manager{
		logger: logger,
		pool:   pool,
	}
}

// RunForGroups the manager on the specified master groups until the context
// expires.
func (m *Manager) RunForGroups(ctx context.Context, masterGroups []MasterGroup) {
	wg := sync.WaitGroup{}
	wg.Add(len(masterGroups))

	var leaderGroup MasterGroup

	for _, masterGroup := range masterGroups {
		leaderGroup = append(leaderGroup, masterGroup[0])

		go func(masterGroup MasterGroup) {
			defer wg.Done()
			m.RunForGroup(ctx, masterGroup)
		}(masterGroup)
	}

	m.RunForGroup(ctx, leaderGroup)
	wg.Wait()
}

// RunForGroup the manager on the specified master group until the context
// expires.
func (m *Manager) RunForGroup(ctx context.Context, masterGroup MasterGroup) {
	m.logger.Log("event", "meeting cluster", "master-group", masterGroup)

	leader := masterGroup[0]

	conn := m.pool.Get(leader)
	conn.Send("MULTI")

	for _, redisInstance := range masterGroup[1:] {
		ipAddresses, err := net.LookupIP(redisInstance.Hostname)

		if err != nil {
			continue
		}

		for _, ipAddress := range ipAddresses {
			conn.Send("CLUSTER", "MEET", ipAddress, redisInstance.Port)
		}
	}

	_, err := conn.Do("EXEC")
	conn.Close()

	if err != nil {
		m.logger.Log("event", "failed to meet cluster", "master-group", masterGroup, "error", err)
	} else {
		m.logger.Log("event", "master-group ready", "master-group", masterGroup)
	}

	<-ctx.Done()
}

func retryUntilSuccess(ctx context.Context, retryPeriod time.Duration, f func() error) error {
	err := f()

	for err != nil {
		timer := time.NewTimer(retryPeriod)

		select {
		case <-timer.C:
			err = f()
			timer.Reset(retryPeriod)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
