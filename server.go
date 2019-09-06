/*
   Copyright 2019 Jan Berktold <jan@berktold.co>

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package memcachey

import (
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/fatih/pool"
	"github.com/serialx/hashring"
)

type connectionProvider interface {
	ForKey(key string) (net.Conn, error)
	ForKeys(keys []string) (map[net.Conn][]string, error)
	ForAddress(address string) (net.Conn, error)
	ForEach() ([]net.Conn, error)
}

type roundRobinConnectionProvider struct {
	pools           []pool.Pool
	poolsByAddress  map[string]pool.Pool
	roundRobinIndex uint64
}

func newRoundRobinConnectionProvider(addresses []string, minCons, maxCons int, connectTimeout time.Duration) (*roundRobinConnectionProvider, error) {
	pools, poolsByAddress, err := buildPools(addresses, minCons, maxCons, connectTimeout)
	if err != nil {
		return nil, err
	}

	return &roundRobinConnectionProvider{
		pools:          pools,
		poolsByAddress: poolsByAddress,
	}, nil
}

func (p *roundRobinConnectionProvider) ForKey(key string) (net.Conn, error) {
	currentIndex := atomic.AddUint64(&p.roundRobinIndex, 1) % uint64(len(p.pools))

	pool := p.pools[currentIndex]

	return pool.Get()
}

func (p *roundRobinConnectionProvider) ForKeys(keys []string) (map[net.Conn][]string, error) {
	result := make(map[net.Conn][]string, 1)

	if conn, err := p.ForKey(""); err == nil {
		result[conn] = keys
	} else {
		return nil, err
	}

	return result, nil
}

func (p *roundRobinConnectionProvider) ForAddress(address string) (net.Conn, error) {
	if pool, ok := p.poolsByAddress[address]; ok {
		return pool.Get()
	}

	return nil, ErrNoSuchAddress
}

func (p *roundRobinConnectionProvider) ForEach() ([]net.Conn, error) {
	result := make([]net.Conn, len(p.pools))

	for i, pool := range p.pools {
		conn, err := pool.Get()
		if err != nil {
			return nil, err
		}

		result[i] = conn
	}

	return result, nil
}

type consistentHashConnnectionProvider struct {
	ring           *hashring.HashRing
	pools          []pool.Pool
	poolsByAddress map[string]pool.Pool
}

func newConsistentHashConnectionProvider(addresses []string, minCons, maxCons int, connectTimeout time.Duration) (*consistentHashConnnectionProvider, error) {
	ring := hashring.New(addresses)

	pools, poolsByAddress, err := buildPools(addresses, minCons, maxCons, connectTimeout)
	if err != nil {
		return nil, err
	}

	return &consistentHashConnnectionProvider{
		ring:           ring,
		pools:          pools,
		poolsByAddress: poolsByAddress,
	}, nil
}

func (p *consistentHashConnnectionProvider) ForKey(key string) (net.Conn, error) {
	address, ok := p.ring.GetNode(key)
	if !ok {
		return nil, errors.New("failed to get slot")
	}

	return p.poolsByAddress[address].Get()
}

func (p *consistentHashConnnectionProvider) ForKeys(keys []string) (map[net.Conn][]string, error) {
	addressToKeys := map[string][]string{}

	for _, key := range keys {
		address, ok := p.ring.GetNode(key)
		if !ok {
			return nil, errors.New("failed to get slot")
		}

		if array, ok := addressToKeys[address]; ok {
			addressToKeys[address] = append(array, key)
		} else {
			addressToKeys[address] = []string{key}
		}
	}

	connectionsToKeys := map[net.Conn][]string{}

	for address, keys := range addressToKeys {
		conn, err := p.poolsByAddress[address].Get()
		if err != nil {
			return nil, err
		}

		connectionsToKeys[conn] = keys
	}
	return connectionsToKeys, nil
}

func (p *consistentHashConnnectionProvider) ForAddress(address string) (net.Conn, error) {
	if pool, ok := p.poolsByAddress[address]; ok {
		return pool.Get()
	}

	return nil, ErrNoSuchAddress
}

func (p *consistentHashConnnectionProvider) ForEach() ([]net.Conn, error) {
	result := make([]net.Conn, len(p.pools))

	for i, pool := range p.pools {
		conn, err := pool.Get()
		if err != nil {
			return nil, err
		}

		result[i] = conn
	}

	return result, nil
}

func buildPools(addresses []string, minCons, maxCons int, connectTimeout time.Duration) ([]pool.Pool, map[string]pool.Pool, error) {
	pools := make([]pool.Pool, len(addresses))
	poolsByAddress := make(map[string]pool.Pool, len(addresses))

	for i, address := range addresses {
		address := address
		p, err := pool.NewChannelPool(minCons, maxCons, func() (net.Conn, error) {
			conn, err := net.DialTimeout("tcp", address, connectTimeout)
			if err != nil {
				return nil, err
			}

			return conn, nil
		})

		if err != nil {
			return nil, nil, err
		}

		pools[i] = p
		poolsByAddress[address] = p
	}

	return pools, poolsByAddress, nil
}
