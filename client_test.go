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
	someByteValue    = []byte{1, 2, 3, 4, 5, 20}
	anotherByteValue = []byte{5, 2, 3, 4, 5, 20}
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
		name: "SetThenGet",
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
	{
		name: "SetWithExpiryAndWaitForExpiration",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			if err := client.SetWithExpiry(key, someByteValue, time.Second); err != nil {
				t.Fatalf("Failed to set key: %v", err)
			}

			time.Sleep(2 * time.Second)

			value, err := client.Get(key)
			if err != nil {
				t.Fatalf("Failed to get key: %v", err)
			}

			if value != nil {
				t.Fatalf("Returned value even though it should have been expired: %v", value)
			}
		},
	},
	{
		name: "AddThenGet",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			if err := client.Add(key, someByteValue); err != nil {
				t.Fatalf("Failed to add key: %v", err)
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
	{
		name: "SetThenAdd",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			if err := client.Set(key, someByteValue); err != nil {
				t.Fatalf("Failed to set key: %v", err)
			}

			if err := client.Add(key, anotherByteValue); err != ErrNotStored {
				t.Fatalf("Expected ErrNotStored, but got %v", err)
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
	{
		name: "ReplaceOnNotSetKey",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			if err := client.Replace(key, someByteValue); err != ErrNotStored {
				t.Fatalf("Expected to get ErrNotStored, got %v", err)
			}

			value, err := client.Get(key)
			if err != nil {
				t.Fatalf("Failed to get key: %v", err)
			}

			if value != nil {
				t.Fatalf("Expected no value to be stored, got %v.", value)
			}
		},
	},
	{
		name: "SetThenReplace",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			if err := client.Set(key, someByteValue); err != nil {
				t.Fatalf("Failed to set key: %v", err)
			}

			if err := client.Replace(key, anotherByteValue); err != nil {
				t.Fatalf("Failed to replace key: %v", err)
			}

			value, err := client.Get(key)
			if err != nil {
				t.Fatalf("Failed to get key: %v", err)
			}

			if !bytes.Equal(anotherByteValue, value) {
				t.Fatalf("Returned response is not equal to expected. Got %v, expected %v.", value, someByteValue)
			}
		},
	},
	{
		name: "AddWithExpiryAndWaitForExpiration",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			if err := client.AddWithExpiry(key, someByteValue, time.Second); err != nil {
				t.Fatalf("Failed to add key: %v", err)
			}

			time.Sleep(2 * time.Second)

			value, err := client.Get(key)
			if err != nil {
				t.Fatalf("Failed to get key: %v", err)
			}

			if value != nil {
				t.Fatalf("Returned value even though it should have been expired: %v", value)
			}
		},
	},
	{
		name: "DeleteNotSetKey",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			existed, err := client.Delete(key)
			if err != nil {
				t.Fatalf("Failed to delete key: %v", err)
			}

			if existed {
				t.Fatal("Expected key to not exist but it did")
			}
		},
	},
	{
		name: "SetThenDelete",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			if err := client.Set(key, someByteValue); err != nil {
				t.Fatalf("Failed to set key: %v", err)
			}

			existed, err := client.Delete(key)
			if err != nil {
				t.Fatalf("Failed to delete key: %v", err)
			}

			if !existed {
				t.Fatal("Expected key to exist but it did not")
			}
		},
	},
	{
		name: "MultiGetWithNoResults",
		test: func(t *testing.T, client *Client) {
			key := memcachedTestKey(t)

			values, err := client.MultiGet([]string{key})
			if err != nil {
				t.Fatalf("Failed to retrieve keys: %v", err)
			}

			if values == nil {
				t.Fatalf("Expected value to be empty map, not nil")
			}

			if len(values) != 0 {
				t.Fatalf("Expected values to be empty but has %v members", len(values))
			}
		},
	},
	{
		name: "MultiGetWithSomeResults",
		test: func(t *testing.T, client *Client) {
			keys := memcachedTestKeys(t, 10)

			for i := 0; i < 5; i++ {
				if err := client.Set(keys[i], someByteValue); err != nil {
					t.Fatalf("Failed to set key: %v", err)
				}
			}

			values, err := client.MultiGet(keys)
			if err != nil {
				t.Fatalf("Failed to retrieve keys: %v", err)
			}

			if values == nil {
				t.Fatalf("Expected value to be map, not nil")
			}

			if len(values) != 5 {
				t.Fatalf("Expected values have 5 members but has %v members", len(values))
			}

			for i := 0; i < 5; i++ {
				if value, ok := values[keys[i]]; ok {
					if !bytes.Equal(value, someByteValue) {
						t.Fatalf("Expected returned value to be %v, got %v.", someByteValue, value)
					}
				} else {
					t.Fatalf("Expected result for key %v to be returned but was not", keys[i])
				}
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

func memcachedTestKeys(t *testing.T, num int) []string {
	result := make([]string, num)

	for i := 0; i < num; i++ {
		result[i] = fmt.Sprintf("%s_something_%d_%d", t.Name(), time.Now().Second(), i)
	}

	return result
}
