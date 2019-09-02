package memcache // import "github.com/janberktold/go-memcache"

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
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

type Client struct {
	cp *connectionProvider
}

func NewClient(addresses []string) (*Client, error) {
	provider, err := newConnectionProvider(addresses)
	if err != nil {
		return nil, err
	}

	return &Client{
		cp: provider,
	}, nil
}

func (c *Client) Set(key string, value []byte) error {
	if err := verifyKey(key); err != nil {
		return err
	}

	connection, err := c.cp.Get()
	if err != nil {
		return err
	}
	defer connection.Close()

	fmt.Println(key)
	if err := writeStorage(connection, "set", key, 0, value); err != nil {
		return err
	}

	if _, err := readStorageResponse(connection); err != nil {
		return err
	}

	return nil
}

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

	return nil, nil
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

func readRetrievalResponse(conn net.Conn) error {
	return nil
}

func verifyKey(key string) error {
	return nil
}
