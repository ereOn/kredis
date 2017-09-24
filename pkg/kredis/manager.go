package kredis

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/go-kit/kit/log"
)

// ManagerState represents a state for the manager.
type ManagerState string

const (
	// ManagerStateDNSResolution indicates that the manager is waiting for all
	// its master groups addresses to resolve.
	ManagerStateDNSResolution = "dns-resolution"
	// ManagerStateMesh indicates that the manager is establishing the mesh.
	ManagerStateMesh = "mesh"
	// ManagerStateReplication indicates that the manager is setting-up
	// replication.
	ManagerStateReplication = "replication"
	// ManagerStateAssignation indicates that the manager is setting-up slots
	// assignations.
	ManagerStateAssignation = "assignation"
	// ManagerStateStable indicates that the cluster is stable.
	ManagerStateStable = "stable"
)

// A Manager is responsible for the coordination of Redis instances inside a
// MasterGroup.
type Manager struct {
	SyncPeriod             time.Duration
	WarningPeriodThreshold time.Duration
	Logger                 log.Logger
	Pool                   *Pool
	MaxSlots               int
	state                  ManagerState
}

func (m *Manager) setState(state ManagerState) {
	if m.state != state {
		m.state = state
		m.Logger.Log("event", "state changed", "state", state)
	}
}

// Run the manager on the specified master groups until the context expires.
func (m *Manager) Run(ctx context.Context, masterGroups []MasterGroup) {
	ticker := time.NewTicker(m.SyncPeriod)
	defer ticker.Stop()

	errorFeed := &ErrorFeed{
		Threshold: m.WarningPeriodThreshold,
	}

	m.setState(ManagerStateDNSResolution)

	for {
		var err error
		var db *Database

		db, err = m.BuildDatabase(ctx, masterGroups)

		if err != nil {
			errorFeed.Add(err)
		} else {
			operations := db.GetOperations()

			if len(operations) > 0 {
				for _, operation := range operations {
					switch operation := operation.(type) {
					case MeetOperation:
						m.setState(ManagerStateMesh)
						m.Logger.Log("event", "cluster meet", "target", operation.Target, "other", operation.Other)
						err = m.ClusterMeet(ctx, operation.Target, operation.Other)

						if err != nil {
							errorFeed.Add(err)
						}
					case ForgetOperation:
						m.setState(ManagerStateMesh)
						m.Logger.Log("event", "cluster forget", "target", operation.Target, "node-id", operation.NodeID)
						err = m.ClusterForget(ctx, operation.Target, operation.NodeID)

						if err != nil {
							errorFeed.Add(err)
						}
					case ReplicateOperation:
						m.setState(ManagerStateReplication)
						m.Logger.Log("event", "cluster replicate", "target", operation.Target, "master", operation.Master)
						err = m.ClusterReplicate(ctx, operation.Target, operation.MasterID)

						if err != nil {
							errorFeed.Add(err)
						}
					case AddSlotsOperation:
						m.setState(ManagerStateAssignation)
						m.Logger.Log("event", "cluster add slots", "target", operation.Target, "slots", operation.Slots)
						err = m.ClusterAddSlots(ctx, operation.Target, operation.Slots)

						if err != nil {
							errorFeed.Add(err)
						}
					case MigrateSlotOperation:
						m.setState(ManagerStateAssignation)
						m.Logger.Log("event", "cluster migrate slot", "source", operation.Source, "destination", operation.Destination, "slot", operation.Slot)
						err = m.ClusterMigrateSlots(ctx, operation.Source, operation.SourceID, operation.Destination, operation.DestinationID, HashSlots{operation.Slot})

						if err != nil {
							errorFeed.Add(err)
						}
					}
				}
			} else {
				m.setState(ManagerStateStable)
			}
		}

		if err == nil {
			errorFeed.Reset()
		} else if errors := errorFeed.PopErrors(); len(errors) != 0 {
			m.Logger.Log("event", "synchronization errors", "errors-count", len(errors))

			for i, item := range errors {
				m.Logger.Log("event", "synchronization error", "error-index", i, "error", item.Error, "count", item.Count)
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

	db = &Database{ManagedSlots: AllSlots}
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

// ClusterForget causes a node to forget another one.
func (m *Manager) ClusterForget(ctx context.Context, redisInstance RedisInstance, nodeID ClusterNodeID) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("asking %s to forget %s: %s", redisInstance, nodeID, err)
		}
	}()

	conn := m.Pool.Get(redisInstance)
	defer conn.Close()

	_, err = conn.Do("CLUSTER", "FORGET", nodeID)

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

// ClusterAddSlots causes a node to take ownership of the specified slots.
func (m *Manager) ClusterAddSlots(ctx context.Context, redisInstance RedisInstance, slots HashSlots) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("asking %s to take ownership of %d slot(s): %s", redisInstance, len(slots), err)
		}
	}()

	conn := m.Pool.Get(redisInstance)
	defer conn.Close()

	for i := 0; i < len(slots); i += m.MaxSlots {
		up := i + m.MaxSlots

		if up > len(slots) {
			up = len(slots)
		}

		args := make([]interface{}, up-i+1)
		args[0] = "ADDSLOTS"

		for j, slot := range slots[i:up] {
			args[j+1] = slot
		}

		err = conn.Send("CLUSTER", args...)

		if err != nil {
			return
		}
	}

	err = conn.Flush()

	if err != nil {
		return
	}

	_, err = conn.Receive()

	return
}

// ClusterMigrateSlots causes slots to migrate from one cluster node to another.
func (m *Manager) ClusterMigrateSlots(ctx context.Context, source RedisInstance, sourceID ClusterNodeID, destination RedisInstance, destinationID ClusterNodeID, slots HashSlots) (err error) {
	keysBatchSize := 10000
	keysCopyTimeout := time.Second * 30

	defer func() {
		if err != nil {
			err = fmt.Errorf("migrating slots %s from %s to %s: %s", slots, source, destination, err)
		}
	}()

	sourceConn := m.Pool.Get(source)
	defer sourceConn.Close()
	destConn := m.Pool.Get(destination)
	defer destConn.Close()

	for _, slot := range slots {
		if _, err = destConn.Do("CLUSTER", "SETSLOT", slot, "IMPORTING", sourceID); err != nil {
			return
		}

		if _, err = sourceConn.Do("CLUSTER", "SETSLOT", slot, "MIGRATING", destinationID); err != nil {
			destConn.Do("CLUSTER", "SETSLOT", slot, "STABLE")
			return
		}

		var keys []string

		for {
			keys, err = redis.Strings(sourceConn.Do("CLUSTER", "GETKEYSINSLOT", slot, keysBatchSize))

			if err != nil {
				destConn.Do("CLUSTER", "SETSLOT", slot, "STABLE")
				sourceConn.Do("CLUSTER", "SETSLOT", slot, "STABLE")
				return
			}

			if len(keys) == 0 {
				break
			}

			args := []interface{}{
				destination.Hostname, destination.Port, "", 0, int(keysCopyTimeout.Seconds()), "REPLACE", "KEYS",
			}

			for _, key := range keys {
				args = append(args, key)
			}

			if _, err = sourceConn.Do("MIGRATE", args...); err != nil {
				destConn.Do("CLUSTER", "SETSLOT", slot, "STABLE")
				sourceConn.Do("CLUSTER", "SETSLOT", slot, "STABLE")
				return
			}
		}

		destConn.Do("CLUSTER", "SETSLOT", slot, "NODE", destinationID)
		sourceConn.Do("CLUSTER", "SETSLOT", slot, "NODE", destinationID)
	}

	return
}
