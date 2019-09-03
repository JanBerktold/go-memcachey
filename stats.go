package memcachey

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Statistics contains general-purpose statistics about a Memcached server.
type Statistics struct {
	// ProcessID is the process id of the server process
	ProcessID uint32 `proto:"pid"`

	// Uptime is the time since the server started
	Uptime time.Duration `proto:"uptime"`

	// Time is the current UNIX time according to the server
	Time uint64 `proto:"time"`

	// Version is the version string reported by the server
	Version string `proto:"version"`

	// PointerSize on the host OS, generally either 32 or 64.
	PointerSize uint32 `proto:"pointer_size"`

	// CurrentItems is the current number of items stored
	CurrentItems uint64 `proto:"curr_items"`

	// TotalItems is the total number of items stored since the server started
	TotalItems uint64 `proto:"total_items"`

	// Bytes is the current number of bytes used to store items
	Bytes uint64 `proto:"bytes"`

	// Max number of  simultaneous connections
	MaxConnections uint32 `proto:"max_connections"`

	// Total number of connections opened since the server started running
	TotalConnections uint32 `proto:"total_connections"`

	// CurrentConenctions is the current number of open connections
	CurrentConnnections uint32 `proto:"curr_connections"`

	// RejectedConnections is the connections rejected in maxconns_fast mode
	RejectedConnections uint64 `proto:"rejected_connections"`

	// ConnectionStructures is the number of connection structures allocated by the server
	ConnectionStructures uint32 `proto:"connection_structures"`

	// Number of misc fds used internally
	ReservedFileDescriptors uint32 `proto:"reserved_fds"`

	// Cumulative number of retrieval reqs
	TotalGetCommands uint64 `proto:"cmd_get"`

	// Cumulative number of storage reqs
	TotalSetCommands uint64 `proto:"cmd_set"`

	// Cumulative number of flush reqs
	TotalFlushCommands uint64 `proto:"cmd_flush"`

	// Cumulative number of touch reqs
	TotalTotalCommands uint64 `proto:"cmd_touch"`

	// Number of keys that have been requested and found present
	GetHits uint64 `proto:"get_hits"`

	// Number of items that have been requested and not found
	GetMisses uint64 `proto:"get_misses"`

	// Number of items that have been requested but had already expired
	GetExpired uint64 `proto:"get_expired"`

	// Number of items that have been requested but have been flushed via flush_all
	GetFlushed uint64 `proto:"get_flushed"`

	// Number of deletion reqs resulting in an item being removed
	DeleteHits uint64 `proto:"delete_hits"`

	// Number of deletions reqs for missing keys
	DeleteMisses uint64 `proto:"delete_misses"`

	// Number of incr reqs against missing keys
	IncrementMisses uint64 `proto:"incr_misses"`

	// Number of successful incr reqs
	IncrementHits uint64 `proto:"incr_hits"`

	// Number of decr reqs against missing keys
	DecrementMisses uint64 `proto:"decr_misses"`

	// Number of successful decr reqs
	DecrementHits uint64 `proto:"decr_hits"`
}

/*
| cas_misses            | 64u     | Number of CAS reqs against missing keys.  |
| cas_hits              | 64u     | Number of successful CAS reqs.            |
| cas_badval            | 64u     | Number of CAS reqs for which a key was    |
|                       |         | found, but the CAS value did not match.   |
| touch_hits            | 64u     | Number of keys that have been touched     |
|                       |         | with a new expiration time                |
| touch_misses          | 64u     | Number of items that have been touched    |
|                       |         | and not found                             |
| auth_cmds             | 64u     | Number of authentication commands         |
|                       |         | handled, success or failure.              |
| auth_errors           | 64u     | Number of failed authentications.         |
| idle_kicks            | 64u     | Number of connections closed due to       |
|                       |         | reaching their idle timeout.              |
| evictions             | 64u     | Number of valid items removed from cache  |
|                       |         | to free memory for new items              |
| reclaimed             | 64u     | Number of times an entry was stored using |
|                       |         | memory from an expired entry              |
| bytes_read            | 64u     | Total number of bytes read by this server |
|                       |         | from network                              |
| bytes_written         | 64u     | Total number of bytes sent by this server |
|                       |         | to network                                |
| limit_maxbytes        | size_t  | Number of bytes this server is allowed to |
|                       |         | use for storage.                          |
| accepting_conns       | bool    | Whether or not server is accepting conns  |
| listen_disabled_num   | 64u     | Number of times server has stopped        |
|                       |         | accepting new connections (maxconns).     |
| time_in_listen_disabled_us                                                  |
|                       | 64u     | Number of microseconds in maxconns.       |
| threads               | 32u     | Number of worker threads requested.       |
|                       |         | (see doc/threads.txt)                     |
| conn_yields           | 64u     | Number of times any connection yielded to |
|                       |         | another due to hitting the -R limit.      |
| hash_power_level      | 32u     | Current size multiplier for hash table    |
| hash_bytes            | 64u     | Bytes currently used by hash tables       |
| hash_is_expanding     | bool    | Indicates if the hash table is being      |
|                       |         | grown to a new size                       |
| expired_unfetched     | 64u     | Items pulled from LRU that were never     |
|                       |         | touched by get/incr/append/etc before     |
|                       |         | expiring                                  |
| evicted_unfetched     | 64u     | Items evicted from LRU that were never    |
|                       |         | touched by get/incr/append/etc.           |
| evicted_active        | 64u     | Items evicted from LRU that had been hit  |
|                       |         | recently but did not jump to top of LRU   |
| slab_reassign_running | bool    | If a slab page is being moved             |
| slabs_moved           | 64u     | Total slab pages moved                    |
| crawler_reclaimed     | 64u     | Total items freed by LRU Crawler          |
| crawler_items_checked | 64u     | Total items examined by LRU Crawler       |
| lrutail_reflocked     | 64u     | Times LRU tail was found with active ref. |
|                       |         | Items can be evicted to avoid OOM errors. |
| moves_to_cold         | 64u     | Items moved from HOT/WARM to COLD LRU's   |
| moves_to_warm         | 64u     | Items moved from COLD to WARM LRU         |
| moves_within_lru      | 64u     | Items reshuffled within HOT or WARM LRU's |
| direct_reclaims       | 64u     | Times worker threads had to directly      |
|                       |         | reclaim or evict items.                   |
| lru_crawler_starts    | 64u     | Times an LRU crawler was started          |
| lru_maintainer_juggles                                                      |
|                       | 64u     | Number of times the LRU bg thread woke up |
| slab_global_page_pool | 32u     | Slab pages returned to global pool for    |
|                       |         | reassignment to other slab classes.       |
| slab_reassign_rescues | 64u     | Items rescued from eviction in page move  |
| slab_reassign_evictions_nomem                                               |
|                       | 64u     | Valid items evicted during a page move    |
|                       |         | (due to no free memory in slab)           |
| slab_reassign_chunk_rescues                                                 |
|                       | 64u     | Individual sections of an item rescued    |
|                       |         | during a page move.                       |
| slab_reassign_inline_reclaim                                                |
|                       | 64u     | Internal stat counter for when the page   |
|                       |         | mover clears memory from the chunk        |
|                       |         | freelist when it wasn't expecting to.     |
| slab_reassign_busy_items                                                    |
|                       | 64u     | Items busy during page move, requiring a  |
|                       |         | retry before page can be moved.           |
| slab_reassign_busy_deletes                                                  |
|                       | 64u     | Items busy during page move, requiring    |
|                       |         | deletion before page can be moved.        |
| log_worker_dropped    | 64u     | Logs a worker never wrote due to full buf |
| log_worker_written    | 64u     | Logs written by a worker, to be picked up |
| log_watcher_skipped   | 64u     | Logs not sent to slow watchers.           |
| log_watcher_sent      | 64u     | Logs written to watchers.                 |
| rusage_user           | 32u.32u | Accumulated user time for this process    |
|                       |         | (seconds:microseconds)                    |
| rusage_system         | 32u.32u | Accumulated system time for this process  |
|                       |         | (seconds:microseconds)                    |
*/

// StatisticsForAddress returns general-purpose statistics about the specified host.
func (c *Client) StatisticsForAddress(address string) (*Statistics, error) {
	connection, err := c.cp.ForAddress(address)
	if err != nil {
		return nil, err
	}
	defer connection.Close()

	if _, err := fmt.Fprint(connection, "stats\r\n"); err != nil {
		return nil, err
	}

	settings := &Statistics{}

	if err := parseSettingsResponse(connection, func(prefix string) interface{} {
		return settings
	}); err != nil {
		return nil, err
	}

	return settings, nil
}

// SettingsStatistics contains details of the settings of the running memcached
type SettingsStatistics struct {
	// MaxBytes represents the maximum number of bytes allowed in the cache.
	MaxBytes uint64 `proto:"maxbytes"`

	// MaxConnections is the maximum number of clients allowed
	MaxConnections uint32 `proto:"maxconns"`

	// TCPPort is the TCP port to listen on.
	TCPPort uint32 `proto:"tcpport"`

	// TCPPort is the UDP port to listen on.
	UDPPort uint32 `proto:"udpport"`

	// The Interface to listen on.
	Interface string `proto:"inter"`

	// Verbosity level
	Verbosity uint32 `proto:"verbosity"`

	// Oldest is the age of the oldest honored object
	Oldest time.Duration `proto:"oldest"`

	// When off, LRU evictions are disabled
	LRUEvictionsEnabled bool `proto:"evictions"`

	// Path to the domain socket (if any).
	DomainSocket string `proto:"domain_socket"`

	// umask for the creation of the domain socket
	Umask uint32 `proto:"umask"`

	// Chunk size growth factor
	ChunkSizeGrowthFactor float64 `proto:"growth_factor"`

	// Minimum space allocated for key+value+flags
	InitialChunkSize uint32 `proto:"chunk_size"`

	// Number of threads (including dispatch)
	NumberOfThreads uint32 `proto:"num_threads"`

	// Stats prefix separator character
	StatsPrefixSeperatorCharacter string `proto:"stat_key_prefix"`

	// If yes, stats detail is enabled
	StatisticsDetailEnabled bool `proto:"detail_enabled"`

	// Max num IO ops processed within an event
	RequestsPerEvent uint32 `proto:"reqs_per_event"`

	// When no, CAS is not enabled for this server
	CASEnabled bool `proto:"cas_enabled"`

	// TCP listen
	TCPBacklog uint32 `proto:"tcp_backlog"`

	// SASL auth requested and enabled
	SASLAuthenticationEnabled bool `proto:"auth_enabled_sasl"`

	// maximum item size
	MaximumItemSize uint32 `proto:"item_size_max"`

	// If fast disconnects are enabled
	FastMaximumConnectionsEnabled bool `proto:"maxconns_fast"`

	// Starting size multiplier for hash table
	InitialHashPower uint32 `proto:"hashpower_init"`

	// SlabReassignAllowed represents whether slab page reassignment is allowed
	SlabReassignAllowed bool `proto:"slab_reassign"`

	// SlabAutomoverMode represents the current mode.
	SlabAutomoverMode SlabsAutomoveMode `proto:"slab_automove"`

	// SlabAutomoverRatio is the ratio limit between young/old slab classes
	SlabAutomoverRatio float64 `proto:"slab_automove_ratio"`

	// SlabAutomoverWindow is an internal algo tunable for automove
	SlabAutomoverWindow uint32 `proto:"slab_automove_window"`

	// MaximumSlabChunkSize is the maximum slab class size (avoid unless necessary)
	MaximumSlabChunkSize uint32 `proto:"slab_chunk_max"`

	// HashAlgorithm is the hash algorithm used for the hash table.
	HashAlgorithm string `proto:"hash_algorithm"`

	// LRUCrawlerEnabled represents whether the background thread running the LRU crawler is running.
	LRUCrawlerEnabled bool `proto:"lru_crawler"`

	// Microseconds to sleep between LRU crawls
	LRUCrawlerSleep uint32 `proto:"lru_crawler_sleep"`

	// Max items to crawl per slab per run
	LRUCrawlerMaximumItems uint32 `proto:"lru_crawler_tocrawl"`

	// Split LRU mode and background threads
	LRUMaintainerThread bool `proto:"lru_maintainer_thread"`

	// Pct of slab memory reserved for HOT LRU
	HotLRUPct uint32 `proto:"hot_lru_pct"`

	// Pct of slab memory reserved for WARM LRU
	WarmLRUPct uint32 `proto:"warm_lru_pct"`

	// Set idle age of HOT LRU to COLD age * this
	MaximumHotFactor float64 `proto:"hot_max_factor"`

	// Set idle age of WARM LRU to COLD age * this
	MaximumWarmFactor float64 `proto:"warm_max_factor"`

	// If yes, items < temporary_ttl use TEMP_LRU
	TemporaryLRUEnabled bool `proto:"temp_lru"`

	// Items with TTL < this are marked temporary
	TemporaryTTL uint32 `proto:"temporary_ttl"`

	// Drop connections that are idle this many seconds (0 disables)
	ConnectionMaximumIdleTime time.Duration `proto:"idle_time"`

	// Size of internal (not socket) write buffer per active watcher connected.
	WatcherWriteBufferSize uint32 `proto:"watcher_logbuf_size"`

	// Size of internal per-worker-thread buffer which the background thread reads from.
	WorkerWriteBufferSize uint32 `proto:"worker_logbuf_size"`

	// If yes, a "stats sizes" histogram is being dynamically tracked.
	TrackingSizesEnabled bool `proto:"track_sizes"`

	// If yes, and available, drop unused syscalls (see seccomp on Linux, pledge on OpenBSD)
	DropPriviliges bool `proto:"drop_privileges"`
}

// SettingsStatisticsForAddress returns details of the settings of the running memcached.
// This is primarily made up of the results of processing commandline options.
func (c *Client) SettingsStatisticsForAddress(address string) (*SettingsStatistics, error) {
	connection, err := c.cp.ForAddress(address)
	if err != nil {
		return nil, err
	}
	defer connection.Close()

	if _, err := fmt.Fprint(connection, "stats settings\r\n"); err != nil {
		return nil, err
	}

	settings := &SettingsStatistics{}

	if err := parseSettingsResponse(connection, func(prefix string) interface{} {
		return settings
	}); err != nil {
		return nil, err
	}

	return settings, nil
}

// ConnectionStatistics contains information about a specific connection.
type ConnectionStatistics struct {
	// Address is the the address of the remote side. For listening
	// sockets this is the listen address. Note that some socket types
	// (such as UNIX-domain) don't have meaningful remote addresses.
	Address string `proto:"addr"`

	// The address of the server. This field is absent for listening sockets.
	ListenAddress string `proto:"listen_addr"`

	// The current state of the connection.
	State string `proto:"state"`

	// The number of seconds since the most recently issued command on the connection.
	// This measures the time since the start of the command, so if "state" indicates a
	// command is currently executing, this will be the number of seconds the current
	// command has been running.
	TimeSinceLastCommand time.Duration `proto:"secs_since_last_cmd"`
}

// ConnectionStatisticsForAddress returns per-connection statistics.
func (c *Client) ConnectionStatisticsForAddress(address string) (map[string]*ConnectionStatistics, error) {
	connection, err := c.cp.ForAddress(address)
	if err != nil {
		return nil, err
	}
	defer connection.Close()

	if _, err := fmt.Fprint(connection, "stats conns\r\n"); err != nil {
		return nil, err
	}

	statistics := map[string]*ConnectionStatistics{}

	// STAT <file descriptor>:<stat> <value>\r\n
	if err := parseSettingsResponse(connection, func(prefix string) interface{} {
		if stats, ok := statistics[prefix]; ok {
			return stats
		}

		stats := &ConnectionStatistics{}
		statistics[prefix] = stats

		return stats
	}); err != nil {
		return nil, err
	}

	return statistics, nil
}

type ItemStatistics struct {
	// Number of items presently stored in this class. Expired items are not excluded.
	Number uint64 `proto:"number"`

	//Number of times an entry was stored using memory from an expired entry.
	ReclaimedTimes uint64 `proto:"reclaimed"`

	// NumberOfHotItems is the number of items presently stored in the HOT LRU.
	NumberOfHotItems uint64 `proto:"number_hot"`

	// NumberOfWarmItems is the number of items presently stored in the WARM LRU.
	NumberOfWarmItems uint64 `proto:"number_warm"`

	// NumberOfColdItems is the number of items presently stored in the COLD LRU.
	NumberOfColdItems uint64 `proto:"number_cold"`

	// NumberOfTemporaryItems is the number of items presently stored in the TEMPORARY LRU.
	NumberOfTemporaryItems uint64 `proto:"number_temp"`

	// OldestHotItem is the age of the oldest item in the hot lru.
	OldestHotItem time.Duration `proto:"age_hot"`

	// OldestWarmItem is the age of the oldest item in the warm lru.
	OldestWarmItem time.Duration `proto:"age_warm"`

	// OldestItem is the age of the oldest item in the lru.
	OldestItem time.Duration `proto:"age"`
}

/*
mem_requested          Number of bytes requested to be stored in this LRU[*]
evicted                Number of times an item had to be evicted from the LRU
                       before it expired.
evicted_nonzero        Number of times an item which had an explicit expire
                       time set had to be evicted from the LRU before it
                       expired.
evicted_time           Seconds since the last access for the most recent item
                       evicted from this class. Use this to judge how
                       recently active your evicted data is.
outofmemory            Number of times the underlying slab class was unable to
                       store a new item. This means you are running with -M or
                       an eviction failed.
tailrepairs            Number of times we self-healed a slab with a refcount
                       leak. If this counter is increasing a lot, please
                       report your situation to the developers.
expired_unfetched      Number of expired items reclaimed from the LRU which
                       were never touched after being set.
evicted_unfetched      Number of valid items evicted from the LRU which were
                       never touched after being set.
evicted_active         Number of valid items evicted from the LRU which were
                       recently touched but were evicted before being moved to
                       the top of the LRU again.
crawler_reclaimed      Number of items freed by the LRU Crawler.
lrutail_reflocked      Number of items found to be refcount locked in the
                       LRU tail.
moves_to_cold          Number of items moved from HOT or WARM into COLD.
moves_to_warm          Number of items moved from COLD to WARM.
moves_within_lru       Number of times active items were bumped within
                       HOT or WARM.
direct_reclaims        Number of times worker threads had to directly pull LRU
                       tails to find memory for a new item.
hits_to_hot
hits_to_warm
hits_to_cold
hits_to_temp           Number of get_hits to each sub-LRU.
*/

// ItemStatisticsForAddress returns information about item storage per slab class.
func (c *Client) ItemStatisticsForAddress(address string) (map[string]*ItemStatistics, error) {
	connection, err := c.cp.ForAddress(address)
	if err != nil {
		return nil, err
	}
	defer connection.Close()

	if _, err := fmt.Fprint(connection, "stats items\r\n"); err != nil {
		return nil, err
	}

	statistics := map[string]*ItemStatistics{}

	// STAT items:<slabclass>:<stat> <value>\r\n
	if err := parseSettingsResponse(connection, func(prefix string) interface{} {
		prefix = prefix[6:]

		if stats, ok := statistics[prefix]; ok {
			return stats
		}

		stats := &ItemStatistics{}
		statistics[prefix] = stats

		return stats
	}); err != nil {
		return nil, err
	}

	return statistics, nil
}

//type SlabStatistics struct {
//}

/*
| chunk_size      | The amount of space each chunk uses. One item will use   |
|                 | one chunk of the appropriate size.                       |
| chunks_per_page | How many chunks exist within one page. A page by         |
|                 | default is less than or equal to one megabyte in size.   |
|                 | Slabs are allocated by page, then broken into chunks.    |
| total_pages     | Total number of pages allocated to the slab class.       |
| total_chunks    | Total number of chunks allocated to the slab class.      |
| get_hits        | Total number of get requests serviced by this class.     |
| cmd_set         | Total number of set requests storing data in this class. |
| delete_hits     | Total number of successful deletes from this class.      |
| incr_hits       | Total number of incrs modifying this class.              |
| decr_hits       | Total number of decrs modifying this class.              |
| cas_hits        | Total number of CAS commands modifying this class.       |
| cas_badval      | Total number of CAS commands that failed to modify a     |
|                 | value due to a bad CAS id.                               |
| touch_hits      | Total number of touches serviced by this class.          |
| used_chunks     | How many chunks have been allocated to items.            |
| free_chunks     | Chunks not yet allocated to items, or freed via delete.  |
| free_chunks_end | Number of free chunks at the end of the last allocated   |
|                 | page.                                                    |
| active_slabs    | Total number of slab classes allocated.                  |
| total_malloced  | Total amount of memory allocated to slab pages.          |
*/

var statPrefix = []byte("STAT ")

func parseSettingsResponse(conn net.Conn, instanceGetter func(prefix string) interface{}) error {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadSlice('\n')
		if err != nil {
			return err
		}

		if bytes.Equal(line, resultEnd) {
			return nil
		}

		if len(line) <= len(statPrefix) {
			return fmt.Errorf("Unexpected response from Memcached, got %q", string(line))
		}

		returnedPrefix := line[0:len(statPrefix)]

		if !bytes.Equal(returnedPrefix, statPrefix) {
			return fmt.Errorf("Unexpected response from Memcached, got %q", string(line))
		}

		cleanedLine := string(line[len(statPrefix):])
		cleanedLine = strings.Trim(cleanedLine, " \n\r")

		parts := strings.Split(cleanedLine, " ")

		if len(parts) != 2 {
			return fmt.Errorf("Unexpected response from Memcached, got %q", cleanedLine)
		}

		prefix, key := splitKey(parts[0])
		value := parts[1]

		instance := instanceGetter(prefix)
		instanceElem := reflect.ValueOf(instance).Elem()
		instanceType := instanceElem.Type()

		for i := 0; i < instanceType.NumField(); i++ {
			field := instanceType.Field(i)
			tag := field.Tag.Get("proto")

			if tag == key {
				switch field.Type.Name() {
				case "uint64":
					value, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						return err
					}

					instanceElem.Field(i).SetUint(value)
				case "uint32":
					value, err := strconv.ParseUint(value, 10, 32)
					if err != nil {
						return err
					}

					instanceElem.Field(i).SetUint(value)
				case "float64":
					value, err := strconv.ParseFloat(value, 64)
					if err != nil {
						return err
					}

					instanceElem.Field(i).SetFloat(value)
				case "bool":
					isTrue := value == "1" || value == "yes"
					instanceElem.Field(i).SetBool(isTrue)
				case "string":
					instanceElem.Field(i).SetString(value)
				case "Duration":
					value, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						return err
					}

					instanceElem.Field(i).Set(reflect.ValueOf(time.Duration(value)))
				case "SlabsAutomoveMode":
					value, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						return err
					}

					instanceElem.Field(i).Set(reflect.ValueOf(SlabsAutomoveMode(value)))
				default:
					return fmt.Errorf("Found correct field for key %v but unsupported type %v", key, field.Type.Name())
				}
			}
		}
	}
}

func splitKey(completeKey string) (prefix, key string) {
	if i := strings.LastIndex(completeKey, ":"); i != -1 {
		return completeKey[0:i], completeKey[i+1 : len(completeKey)]
	}

	return completeKey, completeKey
}
