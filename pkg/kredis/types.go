package kredis

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// A RedisInstance represents Redis instance - either a master or a slave - in
// Kubernetes.
type RedisInstance struct {
	Hostname string
	Port     string
}

func (i RedisInstance) String() string {
	return fmt.Sprintf("%s:%s", i.Hostname, i.Port)
}

// ParseRedisInstance tries to parse a string into a RedisInstance.
func ParseRedisInstance(s string) (RedisInstance, error) {
	var redisInstance RedisInstance
	s = strings.TrimSpace(s)

	if s == "" {
		return redisInstance, errors.New("a RedisInstance cannot be empty")
	}

	components := strings.Split(s, ":")

	switch len(components) {
	case 1:
		redisInstance = RedisInstance{
			Hostname: strings.TrimSpace(components[0]),
			Port:     "6379",
		}
	case 2:
		redisInstance = RedisInstance{
			Hostname: strings.TrimSpace(components[0]),
			Port:     strings.TrimSpace(components[1]),
		}
	default:
		return redisInstance, fmt.Errorf("parsing \"%s\": too many components: \"%v\"", s, components[2:])
	}

	return redisInstance, nil
}

// A MasterGroup represents a list of Redis instances that belong to the same
// logical group.
type MasterGroup []RedisInstance

func (g MasterGroup) String() string {
	var buffer bytes.Buffer

	for i, redisInstance := range g {
		if i > 0 {
			buffer.WriteString(",")
		}

		buffer.WriteString(redisInstance.String())
	}

	return buffer.String()
}

// ParseMasterGroup tries to parse a string into a master group.
func ParseMasterGroup(s string) (MasterGroup, error) {
	s = strings.TrimSpace(s)

	if s == "" {
		return MasterGroup{}, nil
	}

	parts := strings.Split(s, ",")
	masterGroup := make(MasterGroup, 0, len(parts))

	for i, part := range parts {
		redisInstance, err := ParseRedisInstance(part)

		if err != nil {
			return nil, fmt.Errorf("parsing part %d: %s", i, err)
		}

		masterGroup = append(masterGroup, redisInstance)
	}

	return masterGroup, nil
}

// ClusterID represents a cluster ID.
type ClusterID string

func (i ClusterID) String() string {
	if i == "" {
		return "-"
	}

	return string(i)
}

// ClusterNodeAddress represents a cluster node address.
type ClusterNodeAddress struct {
	IP          net.IP
	Port        string
	ClusterPort string
}

var clusterNodeAddressRegexp = regexp.MustCompile(`^([^:]+):([0-9]+)@([0-9]+)$`)

// ParseClusterNodeAddress parse a cluster node address.
func ParseClusterNodeAddress(s string) (result ClusterNodeAddress, err error) {
	matches := clusterNodeAddressRegexp.FindStringSubmatch(s)

	if len(matches) != 4 {
		err = fmt.Errorf("\"%s\" is not a valid cluster node address", s)
		return
	}

	result.IP = net.ParseIP(matches[1])
	result.Port = matches[2]
	result.ClusterPort = matches[3]

	return
}

func (a ClusterNodeAddress) String() string {
	return fmt.Sprintf("%s:%s@%s", a.IP, a.Port, a.ClusterPort)
}

// ClusterNodeFlag represents a cluster node flag.
type ClusterNodeFlag string

const (
	// FlagMyself indicates that this is the current node.
	FlagMyself ClusterNodeFlag = "myself"
	// FlagMaster indicates the node is a master.
	FlagMaster ClusterNodeFlag = "master"
	// FlagSlave indicates the node is a slave.
	FlagSlave ClusterNodeFlag = "slave"
	// FlagProbableFail indicates the node is probably failing.
	FlagProbableFail ClusterNodeFlag = "fail?"
	// FlagFail indicates the node isfailing.
	FlagFail ClusterNodeFlag = "fail"
	// FlagHandshake indicates the node is being contacted.
	FlagHandshake ClusterNodeFlag = "handshake"
	// FlagNoAddress indicates the node has no known address.
	FlagNoAddress ClusterNodeFlag = "noaddr"
	// flagNoFlags is used to indicate the absence of flags.
	flagNoFlags ClusterNodeFlag = "noflags"
)

// ClusterNodeFlags represents a set of cluster node flags.
type ClusterNodeFlags map[ClusterNodeFlag]bool

func (f ClusterNodeFlags) String() string {
	if len(f) == 0 {
		return string(flagNoFlags)
	}

	var s []string

	for flag := range f {
		s = append(s, string(flag))
	}

	sort.Strings(s)

	return strings.Join(s, ",")
}

// ParseClusterNodeFlags parse a list of cluster node flags.
func ParseClusterNodeFlags(s string) (result ClusterNodeFlags, err error) {
	parts := strings.Split(s, ",")
	result = make(ClusterNodeFlags)

	for _, part := range parts {
		flag := ClusterNodeFlag(part)

		switch flag {
		case FlagMyself, FlagMaster, FlagSlave, FlagProbableFail, FlagFail, FlagHandshake, FlagNoAddress:
			result[flag] = true
		case flagNoFlags:
			result = make(ClusterNodeFlags)
			return
		default:
			err = fmt.Errorf("unrecognized flag \"%s\"", part)
			return
		}
	}

	return
}

// ClusterNodeLinkState represents a cluster node link state.
type ClusterNodeLinkState string

const (
	// LinkStateConnected means the node is connected.
	LinkStateConnected ClusterNodeLinkState = "connected"
	// LinkStateDisconnected means the node is not connected.
	LinkStateDisconnected ClusterNodeLinkState = "disconnected"
)

// HashSlots represents a list of hash slots.
type HashSlots []int

func (s HashSlots) String() string {
	var parts []string
	begin := -1
	last := -1

	addSlot := func(begin, end int) {
		if begin != end {
			parts = append(parts, fmt.Sprintf("%d-%d", begin, end))
		} else {
			parts = append(parts, strconv.Itoa(begin))
		}
	}

	for _, slot := range s {
		if begin < 0 {
			begin = slot
			last = slot
			continue
		}

		if slot == last+1 {
			last = slot
			continue
		} else {
			addSlot(begin, last)
			begin = slot
			last = slot
		}
	}

	addSlot(begin, last)

	return strings.Join(parts, " ")
}

// ParseHashSlots parse a hash slot or hash slot range.
func ParseHashSlots(s string) (slots HashSlots, err error) {
	parts := strings.Split(s, "-")

	switch len(parts) {
	case 1, 2:
		var slot int

		for _, part := range parts {
			slot, err = strconv.Atoi(part)

			if err != nil {
				err = fmt.Errorf("parsing \"%s\": %s", s, err)
				return
			}

			slots = append(slots, slot)
		}

		return
	default:
		err = fmt.Errorf("parsing \"%s\": unknown hash slot format", s)
		return
	}
}

// ClusterNode represents a cluster node.
type ClusterNode struct {
	ID           ClusterID
	Address      ClusterNodeAddress
	Flags        ClusterNodeFlags
	MasterID     ClusterID
	PingSent     int
	PongReceived int
	Epoch        int
	LinkState    ClusterNodeLinkState
	Slots        HashSlots
}

// ParseClusterNode parse a single cluster node string, as returned by the
// `CLUSTER NODES` Redis command.
func ParseClusterNode(s string) (result ClusterNode, err error) {
	parts := strings.Split(s, " ")

	if len(parts) < 8 {
		err = fmt.Errorf("parsing \"%s\": not enough parts", s)
		return
	}

	result.ID = ClusterID(parts[0])
	result.Address, err = ParseClusterNodeAddress(parts[1])

	if err != nil {
		err = fmt.Errorf("parsing \"%s\": %s", s, err)
		return
	}

	result.Flags, err = ParseClusterNodeFlags(parts[2])

	if err != nil {
		err = fmt.Errorf("parsing \"%s\": %s", s, err)
		return
	}

	if parts[3] != "-" {
		result.MasterID = ClusterID(parts[3])
	}

	result.PingSent, err = strconv.Atoi(parts[4])

	if err != nil {
		err = fmt.Errorf("parsing \"%s\": %s", s, err)
		return
	}

	result.PongReceived, err = strconv.Atoi(parts[5])

	if err != nil {
		err = fmt.Errorf("parsing \"%s\": %s", s, err)
		return
	}

	result.Epoch, err = strconv.Atoi(parts[6])

	if err != nil {
		err = fmt.Errorf("parsing \"%s\": %s", s, err)
		return
	}

	result.LinkState = ClusterNodeLinkState(parts[7])
	result.Slots = HashSlots{}

	var slots HashSlots

	for _, part := range parts[8:] {
		slots, err = ParseHashSlots(part)

		if err != nil {
			err = fmt.Errorf("parsing \"%s\": %s", s, err)
			return
		}

		result.Slots = append(result.Slots, slots...)
	}

	sort.Ints(result.Slots)

	return
}

func (n ClusterNode) String() string {
	buffer := &bytes.Buffer{}

	fmt.Fprintf(
		buffer,
		"%s %s %s %s %d %d %d %s",
		n.ID.String(),
		n.Address,
		n.Flags.String(),
		n.MasterID.String(),
		n.PingSent,
		n.PongReceived,
		n.Epoch,
		n.LinkState,
	)

	if len(n.Slots) > 0 {
		fmt.Fprintf(buffer, " %s", n.Slots.String())
	}

	return buffer.String()
}
