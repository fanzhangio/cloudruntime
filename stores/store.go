package stores

import (
	"hash/crc32"
	"time"
)

// EnumOptions defines the options for enumerators
type EnumOptions struct {
	PageSize  int
	Partition int
}

// Enumerable is a store which enumerates items
type Enumerable interface {
	Enumerate(EnumOptions) Enumerator
}

// Value is abstract form of stored object
type Value interface {
	TTL() time.Duration
	Unmarshal(out interface{}) error
}

// Enumerator is used to enumerate items
type Enumerator interface {
	Next() ([]Value, error)
}

// KeyValueStore is simple K/V store
type KeyValueStore interface {
	Put(key string, value interface{}, ttl time.Duration) error
	Get(key string) (Value, error)
	Expire(key string, ttl time.Duration) error
	Remove(key string) (Value, error)
}

// KeySet is a set of keys
type KeySet interface {
	Set(id string, exist bool) error
	Has(id string) (bool, error)
}

// PartitionedStore is the store partitions key/value pairs
// EnumOptions.Partition is used to scan keys
type PartitionedStore interface {
	Enumerable
	KeyValueStore
}

// Acquisition is the result of Acquire
type Acquisition interface {
	Acquired() bool
	Owner() string
	TTL() time.Duration
	Refresh(ttl time.Duration) error
	Release() error
}

// OrderedList is an enumerable list which obeys the order when
// keys are inserted
type OrderedList interface {
	Enumerable
	KeySet
}

// Store is the persistent storage for jobs/tasks
type Store interface {
	// Bucket obtains a reference to a partitioned store
	Bucket(name string) PartitionedStore
	// OrderedList obtains a handle to an ordered list
	OrderedList(name string) OrderedList
	// Acquire acquires a lock
	Acquire(name, ownerID string) (Acquisition, error)
}

const (
	// Infinite disables ttl
	Infinite = time.Duration(-1)
	// NoTTL is equivalent to Infinite
	NoTTL = Infinite
	// Partitions is the total number of partitions
	Partitions = 4096
)

// Partition maps id into a partition
func Partition(id string) int {
	return int(crc32.ChecksumIEEE([]byte(id)) % Partitions)
}
