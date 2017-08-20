package kredis

import (
	"reflect"
	"strings"
	"testing"
)

var (
	riA    = RedisInstance{Hostname: "a"}
	riB    = RedisInstance{Hostname: "b"}
	riC    = RedisInstance{Hostname: "c"}
	group  = MasterGroup{riA, riB, riC}
	nodesA = mustParseClusterNodes(`
a 1:1@1 master,myself - 0 0 0 connected 1 2 3
b 1:1@1 slave a 0 0 0 connected
c 1:1@1 slave a 0 0 0 connected
	`)
	nodesAslave = mustParseClusterNodes(`
a 1:1@1 slave,myself c 0 0 0 connected
b 1:1@1 slave c 0 0 0 connected
c 1:1@1 master - 0 0 0 connected
	`)
	nodesB = mustParseClusterNodes(`
a 1:1@1 master - 0 0 0 connected
b 1:1@1 myself,slave a 0 0 0 connected
c 1:1@1 slave a 0 0 0 connected
	`)
	nodesBdualslave = mustParseClusterNodes(`
a 1:1@1 master - 0 0 0 connected
b 1:1@1 myself,slave c 0 0 0 connected
c 1:1@1 master - 0 0 0 connected
	`)
	nodesC = mustParseClusterNodes(`
a 1:1@1 master - 0 0 0 connected
b 1:1@1 slave a 0 0 0 connected
c 1:1@1 myself,slave a 0 0 0 connected
	`)
	nodesCmaster = mustParseClusterNodes(`
a 1:1@1 slave c 0 0 0 connected
b 1:1@1 slave c 0 0 0 connected
c 1:1@1 myself,master - 0 0 0 connected
	`)
	nodesCconflict = mustParseClusterNodes(`
c 1:1@1 myself,slave b 0 0 0 connected
a 1:1@1 slave c 0 0 0 connected
	`)
	nodesCslaveOfNone = mustParseClusterNodes(`
c 1:1@1 myself,slave - 0 0 0 connected
a 1:1@1 slave c 0 0 0 connected
	`)
	nodesNoSelf = mustParseClusterNodes(`
c 1:1@1 slave b 0 0 0 connected
a 1:1@1 slave c 0 0 0 connected
	`)
)

func mustParseClusterNodes(s string) ClusterNodes {
	nodes, err := ParseClusterNodes(strings.TrimSpace(s))

	if err != nil {
		panic(err)
	}

	return nodes
}

func compareOperations(expected []Operation, operations []Operation) bool {
	if len(expected) != len(operations) {
		return false
	}

	for _, expectedOperation := range expected {
		found := false

		for j, operation := range operations {
			if reflect.DeepEqual(expectedOperation, operation) {
				operations = append(operations[:j], operations[j+1:]...)
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func TestDatabaseRegisterGroupDuplicate(t *testing.T) {
	database := &Database{}
	err := database.RegisterGroup(MasterGroup{riA, riB})

	if err != nil {
		t.Errorf("expected no error but got: %s", err)
	}

	err = database.RegisterGroup(MasterGroup{riC, riB})

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseFeedNoMasterGroup(t *testing.T) {
	database := &Database{}
	err := database.Feed(riA, nodesA)

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseFeedNoMatchingMasterGroup(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(MasterGroup{riB, riC})
	err := database.Feed(riA, nodesA)

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseFeed(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	err := database.Feed(riA, nodesA)

	if err != nil {
		t.Errorf("expected no error but got: %s", err)
	}

	err = database.Feed(riB, nodesB)

	if err != nil {
		t.Errorf("expected no error but got: %s", err)
	}

	err = database.Feed(riC, nodesC)

	if err != nil {
		t.Errorf("expected no error but got: %s", err)
	}
}

func TestDatabaseFeedMasterConflict(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	err := database.Feed(riA, nodesA)

	if err != nil {
		t.Errorf("expected no error but got: %s", err)
	}

	err = database.Feed(riB, nodesB)

	if err != nil {
		t.Errorf("expected no error but got: %s", err)
	}

	err = database.Feed(riC, nodesCmaster)

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseFeedSlaveConflict(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	err := database.Feed(riA, nodesAslave)

	if err != nil {
		t.Errorf("expected no error but got: %s", err)
	}

	err = database.Feed(riC, nodesC)

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseFeedDualSlaveConflict(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	err := database.Feed(riA, nodesA)

	if err != nil {
		t.Errorf("expected no error but got: %s", err)
	}

	err = database.Feed(riB, nodesBdualslave)

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseFeedMasterSlaveConflict(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	err := database.Feed(riC, nodesCconflict)

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseFeedSlaveOfNone(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	err := database.Feed(riC, nodesCslaveOfNone)

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseFeedNoSelf(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	err := database.Feed(riC, nodesNoSelf)

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseFeedDualRedisInstance(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	_ = database.Feed(riC, nodesC)
	err := database.Feed(riC, nodesC)

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseFeedDualRegistration(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	err := database.Feed(riC, nodesC)

	if err != nil {
		t.Errorf("expected no error but got: %s", err)
	}

	err = database.Feed(riB, nodesC)

	if err == nil {
		t.Error("expected an error")
	}
}

func TestDatabaseGetMasterOfNoMaster(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(MasterGroup{riA, riB})
	database.Feed(riA, mustParseClusterNodes(`a 1:1@1 master,myself - 0 0 0 connected`))
	database.Feed(riB, mustParseClusterNodes(`b 1:1@1 master,myself - 0 0 0 connected`))
	value := database.GetMasterOf("a")
	var expected ClusterNodeID

	if value != expected {
		t.Errorf("expected \"%s\", got \"%s\"", expected, value)
	}
}

func TestDatabaseGetMasterOfUnknown(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(MasterGroup{riA, riB})
	database.Feed(riA, mustParseClusterNodes(`a 1:1@1 master,myself - 0 0 0 connected`))
	database.Feed(riB, mustParseClusterNodes(`b 1:1@1 master,myself - 0 0 0 connected`))
	value := database.GetMasterOf("c")
	var expected ClusterNodeID

	if value != expected {
		t.Errorf("expected \"%s\", got \"%s\"", expected, value)
	}
}

func TestDatabaseGetOperationsMesh(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	database.Feed(riA, mustParseClusterNodes(`a 1:1@1 master,myself - 0 0 0 connected`))
	database.Feed(riB, mustParseClusterNodes(`b 1:1@1 master,myself - 0 0 0 connected`))
	database.Feed(riC, mustParseClusterNodes(`c 1:1@1 master,myself - 0 0 0 connected`))
	operations := database.GetOperations()
	expected := []Operation{
		MeetOperation{
			Target: riA,
			Other:  riB,
		},
		MeetOperation{
			Target: riA,
			Other:  riC,
		},
		MeetOperation{
			Target: riB,
			Other:  riA,
		},
		MeetOperation{
			Target: riB,
			Other:  riC,
		},
		MeetOperation{
			Target: riC,
			Other:  riA,
		},
		MeetOperation{
			Target: riC,
			Other:  riB,
		},
	}

	if !compareOperations(expected, operations) {
		t.Errorf("expected:\n%v\ngot:\n%v", expected, operations)
	}
}

func TestDatabaseGetOperationsMeshLeaders(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(MasterGroup{riA})
	database.RegisterGroup(MasterGroup{riB})
	database.RegisterGroup(MasterGroup{riC})
	database.Feed(riA, mustParseClusterNodes(`a 1:1@1 master,myself - 0 0 0 connected`))
	database.Feed(riB, mustParseClusterNodes(`b 1:1@1 master,myself - 0 0 0 connected`))
	database.Feed(riC, mustParseClusterNodes(`c 1:1@1 master,myself - 0 0 0 connected`))
	operations := database.GetOperations()
	expected := []Operation{
		MeetOperation{
			Target: riA,
			Other:  riB,
		},
		MeetOperation{
			Target: riA,
			Other:  riC,
		},
		MeetOperation{
			Target: riB,
			Other:  riA,
		},
		MeetOperation{
			Target: riB,
			Other:  riC,
		},
		MeetOperation{
			Target: riC,
			Other:  riA,
		},
		MeetOperation{
			Target: riC,
			Other:  riB,
		},
	}

	if !compareOperations(expected, operations) {
		t.Errorf("expected:\n%v\ngot:\n%v", expected, operations)
	}
}

func TestDatabaseGetOperationsMeshScaleDown(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(MasterGroup{riA, riB})
	database.Feed(riA, mustParseClusterNodes(`
a 1:1@1 master,myself - 0 0 0 connected
b 1:1@1 master - 0 0 0 connected
c 1:1@1 master - 0 0 0 connected
`))
	database.Feed(riB, mustParseClusterNodes(`
a 1:1@1 master - 0 0 0 connected
b 1:1@1 master,myself - 0 0 0 connected
c 1:1@1 master - 0 0 0 connected
`))
	operations := database.GetOperations()
	expected := []Operation{
		ForgetOperation{
			Target: riA,
			NodeID: "c",
		},
		ForgetOperation{
			Target: riB,
			NodeID: "c",
		},
	}

	if !compareOperations(expected, operations) {
		t.Errorf("expected:\n%v\ngot:\n%v", expected, operations)
	}
}

func TestDatabaseGetOperationsReplication(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	database.Feed(riA, mustParseClusterNodes(`
a 1:1@1 master,myself - 0 0 0 connected
b 1:1@1 master - 0 0 0 connected
c 1:1@1 master - 0 0 0 connected
`))
	database.Feed(riB, mustParseClusterNodes(`
a 1:1@1 master - 0 0 0 connected
b 1:1@1 master,myself - 0 0 0 connected
c 1:1@1 master - 0 0 0 connected
`))
	database.Feed(riC, mustParseClusterNodes(`
a 1:1@1 master - 0 0 0 connected
b 1:1@1 master - 0 0 0 connected
c 1:1@1 master,myself - 0 0 0 connected
`))
	operations := database.GetOperations()
	expected := []Operation{
		ReplicateOperation{
			Target:   riB,
			Master:   riA,
			MasterID: "a",
		},
		ReplicateOperation{
			Target:   riC,
			Master:   riA,
			MasterID: "a",
		},
	}

	if !compareOperations(expected, operations) {
		t.Errorf("expected:\n%v\ngot:\n%v", expected, operations)
	}
}

func TestDatabaseGetOperationsReplicationExistingMaster(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	database.Feed(riA, mustParseClusterNodes(`
a 1:1@1 slave,myself b 0 0 0 connected
b 1:1@1 master - 0 0 0 connected
c 1:1@1 master - 0 0 0 connected
`))
	database.Feed(riB, mustParseClusterNodes(`
a 1:1@1 slave b 0 0 0 connected
b 1:1@1 master,myself - 0 0 0 connected
c 1:1@1 master - 0 0 0 connected
`))
	database.Feed(riC, mustParseClusterNodes(`
a 1:1@1 slave b 0 0 0 connected
b 1:1@1 master - 0 0 0 connected
c 1:1@1 master,myself - 0 0 0 connected
`))
	operations := database.GetOperations()
	expected := []Operation{
		ReplicateOperation{
			Target:   riC,
			Master:   riB,
			MasterID: "b",
		},
	}

	if !compareOperations(expected, operations) {
		t.Errorf("expected:\n%v\ngot:\n%v", expected, operations)
	}
}

func TestDatabaseGetOperationsReplicationExistingUnknownMaster(t *testing.T) {
	database := &Database{}
	database.RegisterGroup(group)
	database.Feed(riA, mustParseClusterNodes(`
a 1:1@1 slave,myself d 0 0 0 connected
b 1:1@1 master - 0 0 0 connected
c 1:1@1 master - 0 0 0 connected
`))
	database.Feed(riB, mustParseClusterNodes(`
a 1:1@1 slave d 0 0 0 connected
b 1:1@1 master,myself - 0 0 0 connected
c 1:1@1 master - 0 0 0 connected
`))
	database.Feed(riC, mustParseClusterNodes(`
a 1:1@1 slave d 0 0 0 connected
b 1:1@1 master - 0 0 0 connected
c 1:1@1 master,myself - 0 0 0 connected
`))
	operations := database.GetOperations()
	expected := []Operation{
		ReplicateOperation{
			Target:   riC,
			Master:   riB,
			MasterID: "b",
		},
		ReplicateOperation{
			Target:   riA,
			Master:   riB,
			MasterID: "b",
		},
	}

	if !compareOperations(expected, operations) {
		t.Errorf("expected:\n%v\ngot:\n%v", expected, operations)
	}
}

func TestDatabaseGetOperationsAssignationSingleGroup(t *testing.T) {
	database := &Database{ManagedSlots: AllSlots}
	database.RegisterGroup(group)
	database.Feed(riA, mustParseClusterNodes(`
a 1:1@1 master,myself - 0 0 0 connected
b 1:1@1 slave a 0 0 0 connected
c 1:1@1 slave a 0 0 0 connected
`))
	database.Feed(riB, mustParseClusterNodes(`
a 1:1@1 master - 0 0 0 connected
b 1:1@1 slave,myself a 0 0 0 connected
c 1:1@1 slave a 0 0 0 connected
`))
	database.Feed(riC, mustParseClusterNodes(`
a 1:1@1 master - 0 0 0 connected
b 1:1@1 slave a 0 0 0 connected
c 1:1@1 slave,myself a 0 0 0 connected
`))
	operations := database.GetOperations()
	expected := []Operation{
		AddSlotsOperation{
			Target: riA,
			Slots:  NewHashSlotsFromRange(0, SlotsCount-1, 1),
		},
	}

	if !compareOperations(expected, operations) {
		t.Errorf("expected:\n%v\ngot:\n%v", expected, operations)
	}
}

func TestDatabaseGetOperationsAssignation(t *testing.T) {
	database := &Database{ManagedSlots: AllSlots}
	database.RegisterGroup(MasterGroup{riA})
	database.RegisterGroup(MasterGroup{riB})
	database.Feed(riA, mustParseClusterNodes(`
a 1:1@1 master,myself - 0 0 0 connected
b 1:1@1 master - 0 0 0 connected
`))
	database.Feed(riB, mustParseClusterNodes(`
a 1:1@1 master - 0 0 0 connected
b 1:1@1 master,myself - 0 0 0 connected
`))
	operations := database.GetOperations()
	expected := []Operation{
		AddSlotsOperation{
			Target: riA,
			Slots:  NewHashSlotsFromRange(0, SlotsCount-1, 2),
		},
		AddSlotsOperation{
			Target: riB,
			Slots:  NewHashSlotsFromRange(1, SlotsCount-1, 2),
		},
	}

	if !compareOperations(expected, operations) {
		t.Errorf("expected:\n%v\ngot:\n%v", expected, operations)
	}
}

func TestDatabaseGetOperationsAssignationPreAssigned(t *testing.T) {
	database := &Database{ManagedSlots: NewHashSlotsFromRange(0, 10, 1)}
	database.RegisterGroup(MasterGroup{riA})
	database.RegisterGroup(MasterGroup{riB})
	database.Feed(riA, mustParseClusterNodes(`
a 1:1@1 master,myself - 0 0 0 connected 0 2 4
b 1:1@1 master - 0 0 0 connected 1 3 5
`))
	database.Feed(riB, mustParseClusterNodes(`
a 1:1@1 master - 0 0 0 connected 0 2 4
b 1:1@1 master,myself - 0 0 0 connected 1 3 5
`))
	operations := database.GetOperations()
	expected := []Operation{
		AddSlotsOperation{
			Target: riA,
			Slots:  NewHashSlotsFromRange(6, 10, 2),
		},
		AddSlotsOperation{
			Target: riB,
			Slots:  NewHashSlotsFromRange(7, 10, 2),
		},
	}

	if !compareOperations(expected, operations) {
		t.Errorf("expected:\n%v\ngot:\n%v", expected, operations)
	}
}
