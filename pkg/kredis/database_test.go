package kredis

import (
	"strings"
	"testing"
)

var (
	riA    = RedisInstance{Hostname: "a"}
	riB    = RedisInstance{Hostname: "b"}
	riC    = RedisInstance{Hostname: "c"}
	group  = MasterGroup{riA, riB, riC}
	nodesA = mustParseClusterNodes(`
a 1:1@1 master,myself - 0 0 0 connected
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
