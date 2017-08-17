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
	db, err := m.BuildDatabase(ctx, masterGroups)

	if err != nil {
		m.logger.Log("event", "failed to build database", "error", err)
	}

	m.logger.Log("event", "database built", "database", db)
	<-ctx.Done()
}

// BuildDatabase build the cluster database by querying all the nodes.
func (m *Manager) BuildDatabase(ctx context.Context, masterGroups []MasterGroup) (db *Database, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("can't build database: %s", err)
		}
	}()

	db = &Database{}
	var nodes ClusterNodes

	for _, masterGroup := range masterGroups {
		for _, redisInstance := range masterGroup {
			nodes, err = m.GetClusterNodes(ctx, redisInstance)

			if err != nil {
				return
			}

			err = db.Feed(redisInstance, nodes)

			if err != nil {
				return
			}
		}
	}

	return
}

// GetClusterNodes gets the cluster nodes for the specified redisInstance.
func (m *Manager) GetClusterNodes(ctx context.Context, redisInstance RedisInstance) (nodes ClusterNodes, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("fetching cluster nodes for %s: %s", redisInstance, err)
		}
	}()

	conn := m.pool.Get(redisInstance)
	defer conn.Close()

	var data interface{}
	data, err = conn.Do("CLUSTER", "NODES")

	if err != nil {
		return
	}

	return ParseClusterNodes(string(data.([]byte)))
}
