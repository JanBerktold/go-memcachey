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
	"bytes"
	"fmt"
	"testing"
	"time"
)

var (
	someByteValue = []byte{1, 2, 3, 4, 5, 20}
)

var testCases = []struct {
	name string
	test func(t *testing.T, client *Client)
}{
	{
		name: "SimpleSet",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			if err := client.Set(key, someByteValue); err != nil {
				t.Fatalf("Failed to set key: %v", err)
			}
		},
	},
	{
		name: "SimpleSetThenGet",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			if err := client.Set(key, someByteValue); err != nil {
				t.Fatalf("Failed to set key: %v", err)
			}

			value, err := client.Get(key)
			if err != nil {
				t.Fatalf("Failed to get key: %v", err)
			}

			if !bytes.Equal(someByteValue, value) {
				t.Fatalf("Returned response is not equal to expected. Got %v, expected %v.", value, someByteValue)
			}
		},
	},
}

func TestAgainstMemcached(t *testing.T) {

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewClient([]string{"127.0.0.1:11211"})
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}

			test.test(t, client)
		})
	}
}

func memcachedTestKey(t *testing.T) string {
	return fmt.Sprintf("%s_something_%d", t.Name(), time.Now().Second())
}
