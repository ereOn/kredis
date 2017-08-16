package kredis

import (
	"fmt"
	"sort"
)

// Database represents a cluster database.
type Database struct {
	redisInstancesByID map[ClusterNodeID]RedisInstance
	nodesByID          map[ClusterNodeID]ClusterNodes
	masters            []ClusterNodeID
	slavesByID         map[ClusterNodeID][]ClusterNodeID
}

// Feed the database with new data.
func (d *Database) Feed(redisInstance RedisInstance, nodes ClusterNodes) error {
	node, err := nodes.Self()

	if err != nil {
		return fmt.Errorf("can't feed from nodes of %s: %s", redisInstance, err)
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

	if d.redisInstancesByID == nil {
		d.redisInstancesByID = make(map[ClusterNodeID]RedisInstance)
	}

	d.redisInstancesByID[node.ID] = redisInstance

	if d.nodesByID == nil {
		d.nodesByID = make(map[ClusterNodeID]ClusterNodes)
	}

	d.nodesByID[node.ID] = nodes

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
