package kredis

import (
	"errors"
	"fmt"
	"sort"
)

// SlotsCount represents the maximum number of slots that can be shared by a cluster.
const SlotsCount = 16384

// AllSlots represents all the existing Redis cluster slots.
var AllSlots = NewHashSlotsFromRange(0, SlotsCount-1, 1)

// Database represents a cluster database.
type Database struct {
	masterGroups                []MasterGroup
	masterGroupsByRedisInstance map[RedisInstance]MasterGroup
	redisInstancesByID          map[ClusterNodeID]RedisInstance
	idByRedisInstance           map[RedisInstance]ClusterNodeID
	nodesByID                   map[ClusterNodeID]ClusterNodes
	masters                     []ClusterNodeID
	slavesByID                  map[ClusterNodeID][]ClusterNodeID
	connections                 []Connection
	slotsByID                   map[ClusterNodeID]HashSlots
	ManagedSlots                HashSlots
}

// A Connection represents a link from one node to the other.
type Connection struct {
	From ClusterNodeID
	To   ClusterNodeID
}

// RegisterGroup registers a master group.
func (d *Database) RegisterGroup(masterGroup MasterGroup) error {
	if d.masterGroupsByRedisInstance == nil {
		d.masterGroupsByRedisInstance = make(map[RedisInstance]MasterGroup)
	}

	for _, redisInstance := range masterGroup {
		if other, ok := d.masterGroupsByRedisInstance[redisInstance]; ok {
			return fmt.Errorf("can't register master group %s because %s is already a member of %s", masterGroup, redisInstance, other)
		}
	}

	for _, redisInstance := range masterGroup {
		d.masterGroupsByRedisInstance[redisInstance] = masterGroup
	}

	d.masterGroups = append(d.masterGroups, masterGroup)

	return nil
}

// Feed the database with new data.
//
// If the feeding would lead to a database inconsistency, an error is returned
// and the Database instance should be discarded.
func (d *Database) Feed(redisInstance RedisInstance, nodes ClusterNodes) error {
	if d.masterGroupsByRedisInstance == nil {
		return errors.New("no master group was registered")
	}

	if _, ok := d.masterGroupsByRedisInstance[redisInstance]; !ok {
		return fmt.Errorf("%s is not part of a registered master group", redisInstance)
	}

	selfNode, err := nodes.Self()

	if err != nil {
		return fmt.Errorf("can't feed from nodes of %s: %s", redisInstance, err)
	}

	if d.redisInstancesByID == nil {
		d.redisInstancesByID = make(map[ClusterNodeID]RedisInstance)
		d.idByRedisInstance = make(map[RedisInstance]ClusterNodeID)
	} else if otherRedisInstance, ok := d.redisInstancesByID[selfNode.ID]; ok {
		return fmt.Errorf("refusing to register %s for %s as it is already registered for %s", selfNode.ID, redisInstance, otherRedisInstance)
	}

	if d.nodesByID == nil {
		d.nodesByID = make(map[ClusterNodeID]ClusterNodes)
	}

	if d.slotsByID == nil {
		d.slotsByID = make(map[ClusterNodeID]HashSlots)
	}

	for _, node := range nodes {
		if node.ID != selfNode.ID {
			d.connections = append(d.connections, Connection{
				From: selfNode.ID,
				To:   node.ID,
			})
		} else {
			d.slotsByID[selfNode.ID] = selfNode.Slots
		}

		if node.Flags[FlagMaster] {
			if err = d.addMaster(node.ID); err != nil {
				return err
			}
		} else {
			if err = d.addSlave(node.MasterID, node.ID); err != nil {
				return err
			}
		}
	}

	d.redisInstancesByID[selfNode.ID] = redisInstance
	d.idByRedisInstance[redisInstance] = selfNode.ID
	d.nodesByID[selfNode.ID] = nodes

	return nil
}

func getClusterNodeIDsIndex(id ClusterNodeID, ids []ClusterNodeID) int {
	return sort.Search(len(ids), func(i int) bool {
		return ids[i] >= id
	})
}

func inClusterNodeIDs(id ClusterNodeID, ids []ClusterNodeID) bool {
	i := getClusterNodeIDsIndex(id, ids)

	return i < len(ids) && ids[i] == id
}

// IsMaster checks if the specified cluster ID is a master node.
func (d *Database) IsMaster(id ClusterNodeID) bool {
	return inClusterNodeIDs(id, d.masters)
}

// IsSlave checks if the specified cluster ID is a slave node.
func (d *Database) IsSlave(id ClusterNodeID) ClusterNodeID {
	for masterID, slaves := range d.slavesByID {
		if inClusterNodeIDs(id, slaves) {
			return masterID
		}
	}

	return ""
}

// GetMasterOf gets the master of the specified node, if there is one.
func (d *Database) GetMasterOf(id ClusterNodeID) ClusterNodeID {
	if nodes, ok := d.nodesByID[id]; ok {
		node, _ := nodes.Self()

		return node.MasterID
	}

	return ""
}

func (d *Database) addMaster(id ClusterNodeID) error {
	index := getClusterNodeIDsIndex(id, d.masters)

	if index < len(d.masters) && d.masters[index] == id {
		return nil
	}

	if masterID := d.IsSlave(id); masterID != "" {
		return fmt.Errorf("refusing to register %s as master because he is a slave of %s", id, masterID)
	}

	d.masters = append(d.masters[:index], append([]ClusterNodeID{id}, d.masters[index:]...)...)

	if d.slavesByID == nil {
		d.slavesByID = make(map[ClusterNodeID][]ClusterNodeID)
	}

	d.slavesByID[id] = make([]ClusterNodeID, 0)

	return nil
}

func (d *Database) addSlave(masterID, id ClusterNodeID) error {
	if masterID == "" {
		return fmt.Errorf("refusing to register %s as slave because he just started replication with a master", id)
	}

	if d.IsMaster(id) {
		return fmt.Errorf("refusing to register %s as slave of %s because he is a master", id, masterID)
	}

	for otherMasterID, slaves := range d.slavesByID {
		if masterID != otherMasterID {
			if inClusterNodeIDs(id, slaves) {
				return fmt.Errorf("refusing to register %s as slave of %s because he is already a slave of %s", id, masterID, otherMasterID)
			}
		} else {
			index := getClusterNodeIDsIndex(id, slaves)

			if index == len(slaves) || slaves[index] != id {
				d.slavesByID[masterID] = append(slaves[:index], append([]ClusterNodeID{id}, slaves[index:]...)...)
			}

			return nil
		}
	}

	// If we reach here, it means the master was not registered and needs to be added.
	if err := d.addMaster(masterID); err != nil {
		return fmt.Errorf("refusing to register %s as slave of %s: %s", id, masterID, err)
	}

	d.slavesByID[masterID] = append(d.slavesByID[masterID], id)

	return nil
}

// Operation represents a cluster operation.
type Operation interface{}

// A MeetOperation indicates that a node must meet another.
type MeetOperation struct {
	Target RedisInstance
	Other  RedisInstance
}

// A ForgetOperation indicates that a node must be forgotten.
type ForgetOperation struct {
	Target RedisInstance
	NodeID ClusterNodeID
}

// A ReplicateOperation indicates that a node must replicate another.
type ReplicateOperation struct {
	Target   RedisInstance
	Master   RedisInstance
	MasterID ClusterNodeID
}

// AddSlotsOperation adds slots to a given instance.
type AddSlotsOperation struct {
	Target RedisInstance
	Slots  HashSlots
}

func (d *Database) getExpectedConnections(masterGroup MasterGroup) (connections []Connection) {
	for i, a := range masterGroup {
		for j, b := range masterGroup {
			if i != j {
				connections = append(connections, Connection{
					From: d.idByRedisInstance[a],
					To:   d.idByRedisInstance[b],
				})
			}
		}
	}

	return
}

func (d *Database) getMissingConnections(expectedConnections []Connection) (connections []Connection) {
	for _, expectedConnection := range expectedConnections {
		found := false

		for _, connection := range d.connections {
			if expectedConnection == connection {
				found = true
				break
			}
		}

		if !found {
			connections = append(connections, expectedConnection)
		}
	}

	return
}

// GetOperations returns the operations that need to be performed in order for
// the cluster to meet an acceptable state.
func (d *Database) GetOperations() (operations []Operation) {
	if operations = d.GetMeshOperations(); len(operations) != 0 {
		return
	}

	if operations = append(operations, d.GetReplicationOperations()...); len(operations) != 0 {
		return
	}

	operations = append(operations, d.GetAssignationOperations()...)
	return
}

// GetMeshOperations returns the mesh operations that need to be performed for
// all the members of the cluster to know about each other.
func (d *Database) GetMeshOperations() (operations []Operation) {
	// Cluster mesh.
	var leaderGroup MasterGroup

	for _, masterGroup := range d.masterGroups {
		leaderGroup = append(leaderGroup, masterGroup[0])
		expectedConnections := d.getExpectedConnections(masterGroup)
		missingConnections := d.getMissingConnections(expectedConnections)

		for _, connection := range missingConnections {
			operations = append(operations, MeetOperation{
				Target: d.redisInstancesByID[connection.From],
				Other:  d.redisInstancesByID[connection.To],
			})
		}
	}

	expectedConnections := d.getExpectedConnections(leaderGroup)
	missingConnections := d.getMissingConnections(expectedConnections)

	for _, connection := range missingConnections {
		operations = append(operations, MeetOperation{
			Target: d.redisInstancesByID[connection.From],
			Other:  d.redisInstancesByID[connection.To],
		})
	}

	for nodeID, nodes := range d.nodesByID {
		for _, node := range nodes {
			if _, ok := d.nodesByID[node.ID]; !ok {
				operations = append(operations, ForgetOperation{
					Target: d.redisInstancesByID[nodeID],
					NodeID: node.ID,
				})
			}
		}
	}

	return
}

// GetReplicationOperations returns the replication operations that need to be
// performed for all the members of the cluster to know about their respective
// roles.
func (d *Database) GetReplicationOperations() (operations []Operation) {
	// Master/slave assignations. Only performed once the mesh is established.
	for _, masterGroup := range d.masterGroups {
		var masters []RedisInstance
		var slaves []RedisInstance

		for _, redisInstance := range masterGroup {
			id := d.idByRedisInstance[redisInstance]

			if d.IsMaster(id) {
				masters = append(masters, redisInstance)
			} else {
				slaves = append(slaves, redisInstance)
			}
		}

		switch len(masters) {
		case 0:
			// No masters. This is weird: let it stabilize.
		case 1:
			// One master. This is expected. Do nothing.
		default:
			// More than one master: we need to demote some to slave.
			var master RedisInstance = masters[0]

			replicators := masters

			for _, slave := range slaves {
				nodeID := d.GetMasterOf(d.idByRedisInstance[slave])

				if nodeID != "" {
					if node, ok := d.redisInstancesByID[nodeID]; ok {
						master = node
						break
					} else {
						// The slave has an unknown master. We must also reassign him.
						replicators = append(replicators, slave)
					}
				}
			}

			for _, slave := range replicators {
				if slave != master {
					operations = append(operations, ReplicateOperation{
						Target:   slave,
						Master:   masters[0],
						MasterID: d.idByRedisInstance[masters[0]],
					})
				}
			}
		}
	}

	return
}

// GetAssignationOperations returns the assignation operations that need to be
// performed for all the members of the cluster to know which slots they are
// responsible for.
func (d *Database) GetAssignationOperations() (operations []Operation) {
	addSlotsByID := map[ClusterNodeID]HashSlots{}
	idsBySlot := map[int]ClusterNodeID{}

	for _, nodeID := range d.masters {
		for _, slot := range d.slotsByID[nodeID] {
			idsBySlot[slot] = nodeID
		}
	}

	for i, slot := range d.ManagedSlots {
		nodeID := d.masters[i%len(d.masters)]

		if ownerID, ok := idsBySlot[slot]; ok {
			if ownerID != nodeID {
				// TODO: Migrate the slot.
			}
		} else {
			addSlotsByID[nodeID] = append(addSlotsByID[nodeID], slot)
		}
	}

	for nodeID, slots := range addSlotsByID {
		operations = append(operations, AddSlotsOperation{
			Target: d.redisInstancesByID[nodeID],
			Slots:  slots,
		})
	}

	return
}
