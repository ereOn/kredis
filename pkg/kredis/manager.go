package kredis

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/go-kit/kit/log"
)

// A Manager is responsible for the coordination of Redis instances inside a
// MasterGroup.
type Manager struct {
	SyncPeriod             time.Duration
	WarningPeriodThreshold time.Duration
	Logger                 log.Logger
	Pool                   *Pool
}

// Run the manager on the specified master groups until the context expires.
func (m *Manager) Run(ctx context.Context, masterGroups []MasterGroup) {
	ticker := time.NewTicker(m.SyncPeriod)
	defer ticker.Stop()

	zero := time.Time{}
	lastFailure := zero
	var errors []error

	for {
		db, err := m.BuildDatabase(ctx, masterGroups)

		if err != nil {
			now := time.Now().UTC()

			if lastFailure == zero {
				lastFailure = now
				errors = []error{err}
			} else if now.After(lastFailure.Add(m.WarningPeriodThreshold)) {
				lastFailure = now
				errors = append(errors, err)

				m.Logger.Log("event", "synchronization errors", "errors-count", len(errors))

				for i, err := range errors {
					m.Logger.Log("event", "synchronization error", "error-index", i, "error", err)
				}

				errors = nil
			}
		} else {
			lastFailure = zero
			errors = nil
			operations := db.GetOperations()

			for _, operation := range operations {
				switch operation := operation.(type) {
				case MeetOperation:
					m.Logger.Log("event", "cluster meet", "target", operation.Target, "other", operation.Other)
					err := m.ClusterMeet(ctx, operation.Target, operation.Other)

					if err != nil {
						m.Logger.Log("event", "synchronization failure", "error", err)
					}
				case ReplicateOperation:
					m.Logger.Log("event", "cluster replicate", "target", operation.Target, "master", operation.Master)
					err := m.ClusterReplicate(ctx, operation.Target, operation.MasterID)

					if err != nil {
						m.Logger.Log("event", "synchronization failure", "error", err)
					}
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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
		if err = db.RegisterGroup(masterGroup); err != nil {
			return
		}

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

	conn := m.Pool.Get(redisInstance)
	defer conn.Close()

	var data interface{}
	data, err = conn.Do("CLUSTER", "NODES")

	if err != nil {
		return
	}

	return ParseClusterNodes(string(data.([]byte)))
}

// ClusterMeet causes a node to meet another one.
func (m *Manager) ClusterMeet(ctx context.Context, redisInstance RedisInstance, other RedisInstance) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("asking %s to meet %s: %s", redisInstance, other, err)
		}
	}()

	conn := m.Pool.Get(redisInstance)
	defer conn.Close()

	var ipAddresses []net.IP
	ipAddresses, err = net.LookupIP(other.Hostname)

	if err != nil {
		return
	}

	_, err = conn.Do("CLUSTER", "MEET", ipAddresses[0], other.Port)

	return
}

// ClusterReplicate causes a node to replicate another one.
func (m *Manager) ClusterReplicate(ctx context.Context, redisInstance RedisInstance, master ClusterNodeID) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("asking %s to replicate %s: %s", redisInstance, master, err)
		}
	}()

	conn := m.Pool.Get(redisInstance)
	defer conn.Close()

	_, err = conn.Do("CLUSTER", "REPLICATE", master)

	return
}
