package kredis

import (
	"net"
	"reflect"
	"testing"
)

func TestParseRedisInstance(t *testing.T) {
	testCases := []struct {
		S              string
		Expected       *RedisInstance
		ExpectedString string
	}{
		{
			"",
			nil,
			"",
		},
		{
			"myredis-0-1",
			&RedisInstance{
				Hostname: "myredis-0-1",
				Port:     "6379",
			},
			"myredis-0-1:6379",
		},
		{
			"myredis-0-1:6380",
			&RedisInstance{
				Hostname: "myredis-0-1",
				Port:     "6380",
			},
			"myredis-0-1:6380",
		},
		{
			"myredis-0-1:6380:bug",
			nil,
			"",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.S, func(t *testing.T) {
			value, err := ParseRedisInstance(testCase.S)

			if testCase.Expected == nil {
				if err == nil {
					t.Fatal("expected an error")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got: %s", err)
				}

				if *testCase.Expected != value {
					t.Errorf("expected:\n%v\ngot:\n%v", *testCase.Expected, value)
				}

				valueStr := value.String()

				if valueStr != testCase.ExpectedString {
					t.Errorf("expected:\n%s\ngot:\n%s", testCase.ExpectedString, valueStr)
				}
			}
		})
	}
}
func TestParseMasterGroup(t *testing.T) {
	testCases := []struct {
		S              string
		Expected       MasterGroup
		ExpectedString string
	}{
		{
			"",
			MasterGroup{},
			"",
		},
		{
			"myredis-0-1",
			MasterGroup{
				{
					Hostname: "myredis-0-1",
					Port:     "6379",
				},
			},
			"myredis-0-1:6379",
		},
		{
			"myredis-0-1:6380,myredis-0-2",
			MasterGroup{
				{
					Hostname: "myredis-0-1",
					Port:     "6380",
				},
				{
					Hostname: "myredis-0-2",
					Port:     "6379",
				},
			},
			"myredis-0-1:6380,myredis-0-2:6379",
		},
		{
			"myredis-0-1:6380:bug",
			nil,
			"",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.S, func(t *testing.T) {
			value, err := ParseMasterGroup(testCase.S)

			if testCase.Expected == nil {
				if err == nil {
					t.Fatal("expected an error")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got: %s", err)
				}

				if !reflect.DeepEqual(testCase.Expected, value) {
					t.Errorf("expected:\n%v\ngot:\n%v", testCase.Expected, value)
				}

				valueStr := value.String()

				if valueStr != testCase.ExpectedString {
					t.Errorf("expected:\n%s\ngot:\n%s", testCase.ExpectedString, valueStr)
				}
			}
		})
	}
}

func TestParseClusterNode(t *testing.T) {
	testCases := []struct {
		S              string
		Expected       *ClusterNode
		ExpectedString string
	}{
		{
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 handshake - 0 0 0 connected",
			&ClusterNode{
				ID: "b4b2de84dfaecb05ab4d32488ede2517fb95aced",
				Address: ClusterNodeAddress{
					IP:          net.ParseIP("127.0.0.2"),
					Port:        "6379",
					ClusterPort: "16379",
				},
				Flags: ClusterNodeFlags{
					FlagHandshake: true,
				},
				MasterID:     "",
				PingSent:     0,
				PongReceived: 0,
				Epoch:        0,
				LinkState:    LinkStateConnected,
				Slots:        HashSlots{},
			},
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 handshake - 0 0 0 connected",
		},
		{
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 myself,master - 2 3 4 disconnected 3 1 5-6",
			&ClusterNode{
				ID: "b4b2de84dfaecb05ab4d32488ede2517fb95aced",
				Address: ClusterNodeAddress{
					IP:          net.ParseIP("127.0.0.2"),
					Port:        "6379",
					ClusterPort: "16379",
				},
				Flags: ClusterNodeFlags{
					FlagMyself: true,
					FlagMaster: true,
				},
				MasterID:     "",
				PingSent:     2,
				PongReceived: 3,
				Epoch:        4,
				LinkState:    LinkStateDisconnected,
				Slots:        HashSlots{1, 3, 5, 6},
			},
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 master,myself - 2 3 4 disconnected 1 3 5-6",
		},
		{
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 noflags abc 2 3 4 disconnected 1 3 5-6 8",
			&ClusterNode{
				ID: "b4b2de84dfaecb05ab4d32488ede2517fb95aced",
				Address: ClusterNodeAddress{
					IP:          net.ParseIP("127.0.0.2"),
					Port:        "6379",
					ClusterPort: "16379",
				},
				Flags:        ClusterNodeFlags{},
				MasterID:     "abc",
				PingSent:     2,
				PongReceived: 3,
				Epoch:        4,
				LinkState:    LinkStateDisconnected,
				Slots:        HashSlots{1, 3, 5, 6, 8},
			},
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 noflags abc 2 3 4 disconnected 1 3 5-6 8",
		},
		{
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced invalid slave abc 2 3 4 disconnected 1 3 5-6 8",
			nil,
			"",
		},
		{
			"invalid",
			nil,
			"",
		},
		{
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 invalid abc 2 3 4 disconnected 1 3 5-6 8",
			nil,
			"",
		},
		{
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 noflags abc a 3 4 disconnected 1 3 5-6 8",
			nil,
			"",
		},
		{
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 noflags abc 2 b 4 disconnected 1 3 5-6 8",
			nil,
			"",
		},
		{
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 noflags abc 2 3 c disconnected 1 3 5-6 8",
			nil,
			"",
		},
		{
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 noflags abc 2 3 4 disconnected 1 a 5-6 8",
			nil,
			"",
		},
		{
			"b4b2de84dfaecb05ab4d32488ede2517fb95aced 127.0.0.2:6379@16379 noflags abc 2 3 4 disconnected 1 3 5-6-7 8",
			nil,
			"",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.S, func(t *testing.T) {
			value, err := ParseClusterNode(testCase.S)

			if testCase.Expected == nil {
				if err == nil {
					t.Fatal("expected an error")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got: %s", err)
				}

				if !reflect.DeepEqual(*testCase.Expected, value) {
					t.Errorf("expected:\n%v\ngot:\n%v", testCase.Expected, value)
				}

				valueStr := value.String()

				if valueStr != testCase.ExpectedString {
					t.Errorf("expected:\n%s\ngot:\n%s", testCase.ExpectedString, valueStr)
				}
			}
		})
	}
}
