package kredis

import (
	"errors"
	"fmt"
	"sort"
)

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

	for _, node := range nodes {
		if node.ID != selfNode.ID {
			d.connections = append(d.connections, Connection{
				From: selfNode.ID,
				To:   node.ID,
			})
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

	//for _, masterGroup := range d.masterGroups {
	//	var masters []redisInstance
	//	var slaves []redisInstance

	//	for _, redisInstance := range masterGroup {
	//		id := d.idByRedisInstance[redisInstance]

	//		if d.IsMaster(id) {
	//			masters = append(masters, redisInstance)
	//		} else {
	//			slaves = append(slaves, redisInstance)
	//		}
	//	}
	//}

	return
}
