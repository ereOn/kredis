package kredis

import (
	"bytes"
	"errors"
	"fmt"
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
