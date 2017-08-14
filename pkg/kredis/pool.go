package kredis

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// A Pool represents a pool of Redis connection pools.
type Pool struct {
	lock        sync.Mutex
	pools       map[RedisInstance]*redis.Pool
	MaxIdle     int
	MaxActive   int
	IdleTimeout time.Duration
	Wait        bool
}

// Get a connection to the specified Redis instance.
func (p *Pool) Get(redisInstance RedisInstance) redis.Conn {
	p.lock.Lock()

	if p.pools == nil {
		p.pools = make(map[RedisInstance]*redis.Pool)
	}

	pool := p.pools[redisInstance]

	if pool == nil {
		pool = &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", redisInstance.String())
			},
			MaxIdle:     p.MaxIdle,
			MaxActive:   p.MaxActive,
			IdleTimeout: p.IdleTimeout,
			Wait:        p.Wait,
		}
		p.pools[redisInstance] = pool
	}

	p.lock.Unlock()

	return pool.Get()
}

type closeError []error

func (e closeError) Error() string {
	var buffer bytes.Buffer

	buffer.WriteString("closing pool:")

	for _, err := range e {
		buffer.WriteString("\n - ")
		buffer.WriteString(err.Error())
	}

	return buffer.String()
}

// Close the pool of connections.
func (p *Pool) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	errors := make(closeError, 0)

	for redisInstance, pool := range p.pools {
		err := pool.Close()

		if err != nil {
			errors = append(errors, fmt.Errorf("closing pool for %s: %s", redisInstance, err))
		}
	}

	p.pools = nil

	if len(errors) > 0 {
		return errors
	}

	return nil
}
