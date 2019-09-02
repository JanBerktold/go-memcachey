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
