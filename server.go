package memcache

import (
	"net"
	"sync/atomic"

	"github.com/fatih/pool"
)

type connectionProvider struct {
	pools           []pool.Pool
	roundRobinIndex uint64
}

func newConnectionProvider(addresses []string) (*connectionProvider, error) {
	pools := make([]pool.Pool, len(addresses))
	for i, address := range addresses {
		address := address
		p, err := pool.NewChannelPool(5, 30, func() (net.Conn, error) {
			return net.Dial("tcp", address)
		})

		if err != nil {
			return nil, err
		}

		pools[i] = p
	}

	return &connectionProvider{
		pools: pools,
	}, nil
}

func (p *connectionProvider) Get() (net.Conn, error) {
	currentIndex := atomic.AddUint64(&p.roundRobinIndex, 1) % uint64(len(p.pools))

	pool := p.pools[currentIndex]

	return pool.Get()
}
