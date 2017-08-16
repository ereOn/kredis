package kredis

import (
	"context"
	"fmt"

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

// Run the manager on the specified master groups until the context expires.
func (m *Manager) Run(ctx context.Context, masterGroups []MasterGroup) {
	m.GetClusterNodes(ctx, masterGroups[0][0])
}

// GetClusterNodes gets the cluster nodes for the specified redisInstance.
func (m *Manager) GetClusterNodes(ctx context.Context, redisInstance RedisInstance) {
	conn := m.pool.Get(redisInstance)
	defer conn.Close()

	data, err := conn.Do("CLUSTER", "NODES")
	fmt.Println(string(data.([]byte)), err)
}
