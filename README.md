# go-memcachey
[![GoDoc](https://godoc.org/github.com/janberktold/go-memcachey?status.svg)](https://godoc.org/github.com/janberktold/go-memcachey)
[![CircleCI](https://circleci.com/gh/JanBerktold/go-memcachey.svg?style=svg)](https://circleci.com/gh/JanBerktold/go-memcachey)

go-memcachey is a [Golang](https://golang.org/) client library for the Memcached in-memory database. See [godoc](https://godoc.org/github.com/janberktold/go-memcachey) for documentation.

# Example

```go
import "github.com/janberktold/go-memcachey"

func main() {
    client, _ := memcachey.NewClient([]string{"127.0.0.1:11211"})

    // Set a value to Memcached
    client.Get("some_key", []byte{1, 2, 3})

    // Read the value back
    value, _ := client.Get("some_key")

    fmt.Printf("Read %v from Memcached!", value)
}
```

# Contributing
## Testing
This client includes a testing suite which runs commands against a Memcached server running at `127.0.0.1:11211`. It is run on every opened pull request, but it is recommended to test locally beforehand. The easiest way to spin up the required Memcached server is to use Docker:
```
docker run --publish 127.0.0.1:11211:11211 memcached:1.5.17-alpine -v -v -v
```

After which you can execute the tests any number of times:
```
go test -v
```