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

// Package memcachey provides a modern, scalable client for the Memcached database.
package memcachey // import "github.com/janberktold/go-memcachey"

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type writeStorageResultType int8

const (
	writeStorageResultTypeStored writeStorageResultType = iota
	writeStorageResultTypeNotStored
	writeStorageResultTypeExists
	writeStorageResultTypeNotFound
)

var (
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultTouched   = []byte("TOUCHED\r\n")
)

type ClientOptionsSetter func(client *Client) error

// WithTimeouts allows specifying custom timeouts for the client.
func WithTimeouts(connectionTimeout, readTimeout, writeTimeout time.Duration) ClientOptionsSetter {
	return func(client *Client) error {
		if connectionTimeout <= 0 {
			return fmt.Errorf("connectionTimeout has invalid value: %v", connectionTimeout)
		}
		if readTimeout <= 0 {
			return fmt.Errorf("readTimeout has invalid value: %v", readTimeout)
		}
		if writeTimeout <= 0 {
			return fmt.Errorf("writeTimeout has invalid value: %v", writeTimeout)
		}

		return nil
	}
}

// WithPoolLimitsPerHost allows specifying custom min and max limits for the
// connection pool on a per-host basis.
func WithPoolLimitsPerHost(min, max int) ClientOptionsSetter {
	return func(client *Client) error {
		return nil
	}
}

// Client is our interface over the logical connection to several Memcached hosts.
type Client struct {
	cp *connectionProvider
}

func NewClient(addresses []string, options ...ClientOptionsSetter) (*Client, error) {
	client := &Client{}

	for _, setter := range options {
		if err := setter(client); err != nil {
			return nil, err
		}
	}

	provider, err := newConnectionProvider(addresses)
	if err != nil {
		return nil, err
	}

	client.cp = provider

	return client, nil
}

// Set sets a key on the Memcached server, regardless of the previous state.
func (c *Client) Set(key string, value []byte) error {
	if err := verifyKey(key); err != nil {
		return err
	}

	connection, err := c.cp.Get()
	if err != nil {
		return err
	}
	defer connection.Close()

	if err := writeStorage(connection, "set", key, 0, value); err != nil {
		return err
	}

	if _, err := readStorageResponse(connection); err != nil {
		return err
	}

	return nil
}

// SetWithExpiry sets a key on the Memcached server which expires after a duration, regardless of the previous state.
func (c *Client) SetWithExpiry(key string, value []byte, expiry time.Duration) error {
	if err := verifyKey(key); err != nil {
		return err
	}

	connection, err := c.cp.Get()
	if err != nil {
		return err
	}
	defer connection.Close()

	if err := writeStorage(connection, "set", key, expiry, value); err != nil {
		return err
	}

	if _, err := readStorageResponse(connection); err != nil {
		return err
	}

	return nil
}

// Add sets a key on the Memcached server only if the key did not have a value previously.
func (c *Client) Add(key string, value []byte) error {
	if err := verifyKey(key); err != nil {
		return err
	}

	connection, err := c.cp.Get()
	if err != nil {
		return err
	}
	defer connection.Close()

	if err := writeStorage(connection, "add", key, 0, value); err != nil {
		return err
	}

	if _, err := readStorageResponse(connection); err != nil {
		return err
	}

	return nil
}

// AddWithExpiry sets a key on the Memcached server which expires after the specified time,
// only if the key did not have a value previously.
func (c *Client) AddWithExpiry(key string, value []byte, expiry time.Duration) error {
	if err := verifyKey(key); err != nil {
		return err
	}

	connection, err := c.cp.Get()
	if err != nil {
		return err
	}
	defer connection.Close()

	if err := writeStorage(connection, "add", key, expiry, value); err != nil {
		return err
	}

	if _, err := readStorageResponse(connection); err != nil {
		return err
	}

	return nil
}

// Delete marks a key as deleted in memcached.
// Returns true if the key existed.
func (c *Client) Delete(key string) (existed bool, err error) {
	if err := verifyKey(key); err != nil {
		return false, err
	}

	connection, err := c.cp.Get()
	if err != nil {
		return false, err
	}
	defer connection.Close()

	if _, err := fmt.Fprintf(connection, "delete %s\r\n", key); err != nil {
		return false, err
	}

	expectedResponses := [][]byte{resultNotFound, resultDeleted}
	response, err := readGenericResponse(connection, expectedResponses)
	if err != nil {
		return false, err
	}

	// TODO: This could be a simpler check.
	return bytes.Equal(response, resultDeleted), nil
}

func readGenericResponse(conn net.Conn, expectedResponses [][]byte) ([]byte, error) {
	w := bufio.NewReader(conn)

	line, err := w.ReadSlice('\n')
	if err != nil {
		return nil, err
	}

	for _, response := range expectedResponses {
		if bytes.Equal(response, line) {
			return response, nil
		}
	}

	return nil, fmt.Errorf("Unexpected response from memcached: %q", line)
}

// Get queries memcached for a single key and returns the value.
func (c *Client) Get(key string) ([]byte, error) {
	if err := verifyKey(key); err != nil {
		return nil, err
	}

	connection, err := c.cp.Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()

	if err := writeRetrieval(connection, "get", []string{key}); err != nil {
		return nil, err
	}

	values, err := readRetrievalResponse(connection)
	if err != nil {
		return nil, err
	}

	value, _ := values[key]
	return value, nil
}

// As per https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L149
// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
// <data block>\r\n
func writeStorage(conn net.Conn, cmd, key string, expirationTime time.Duration, value []byte) error {
	w := bufio.NewWriter(conn)

	if _, err := fmt.Fprintf(w, "%s %s 0 %v %v\r\n", cmd, key, int(expirationTime.Seconds()), len(value)); err != nil {
		return err
	}

	// TODO: What if we wrote less than we wanted to?
	_, err := w.Write(value)
	if err != nil {
		return err
	}

	_, err = w.WriteString("\r\n")
	if err != nil {
		return err
	}

	return w.Flush()
}

func readStorageResponse(conn net.Conn) (writeStorageResultType, error) {
	buffer := make([]byte, 25)

	writtenBytes, err := conn.Read(buffer)
	if err != nil {
		return 0, err
	}

	line := buffer[0:writtenBytes]

	switch {
	case bytes.Equal(line, resultStored):
		return writeStorageResultTypeStored, nil
	case bytes.Equal(line, resultNotStored):
		return writeStorageResultTypeNotStored, nil
	case bytes.Equal(line, resultExists):
		return writeStorageResultTypeExists, nil
	case bytes.Equal(line, resultNotFound):
		return writeStorageResultTypeNotFound, nil
	default:
		return 0, fmt.Errorf("Unexpected response from memcached: %q", string(line))
	}
}

// As per https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L233
// get <key>*\r\n
func writeRetrieval(conn net.Conn, cmd string, keys []string) error {
	w := bufio.NewWriter(conn)

	if _, err := w.WriteString(cmd); err != nil {
		return err
	}

	for _, key := range keys {
		if _, err := fmt.Fprintf(w, " %s", key); err != nil {
			return err
		}
	}

	if _, err := w.WriteString("\r\n"); err != nil {
		return err
	}

	return w.Flush()
}

func readRetrievalResponse(conn net.Conn) (map[string][]byte, error) {
	rd := bufio.NewReader(conn)
	result := make(map[string][]byte, 10)

	for {
		line, err := rd.ReadSlice('\n')
		if err != nil {
			return nil, err
		}

		if bytes.Equal(line, resultEnd) {
			return result, nil
		}

		isValueLine, key, _, valueLength, _ := readValueResponseLine(line)
		if !isValueLine {
			return nil, fmt.Errorf("Unexpected response from memcached: %v", key)
		}

		value := make([]byte, valueLength+2)
		if _, err := rd.Read(value); err != nil {
			return nil, err
		}

		result[key] = value[0:valueLength]
	}
}

var valueLineMarker = []byte("VALUE ")

func readValueResponseLine(line []byte) (isValue bool, key string, flags int, valueLength int, cas int) {
	if len(line) <= len(valueLineMarker) || !bytes.Equal(line[0:len(valueLineMarker)], valueLineMarker) {
		return false, "", 0, 0, 0
	}

	arguments := string(line[len(valueLineMarker):])
	arguments = strings.TrimRight(arguments, "\r\n ")

	parts := strings.Split(arguments, " ")

	if len(parts) != 3 && len(parts) != 4 {
		return false, "", 0, 0, 0
	}

	isValue = true
	key = parts[0]
	flags = 0
	valueLength, err := strconv.Atoi(parts[2])
	if err != nil {
		return false, "", 0, 0, 0
	}

	if len(parts) == 4 {
		cas, err = strconv.Atoi(parts[3])
		if err != nil {
			return false, "", 0, 0, 0
		}
	}

	return true, key, flags, valueLength, cas
}

func verifyKey(key string) error {
	return nil
}
