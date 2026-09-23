// Package kv defines a minimal strongly consistent, versioned key-value store.
// Atomicity is per key; there are no cross-key transaction guarantees.
package kv

import (
	"context"
	"time"
)

// KeyValueKey identifies a record by the pair (PartitionKey, SortKey) within
// a configured store. Components are case-sensitive valid UTF-8 strings.
// PartitionKey must be nonempty; SortKey may be empty. Providers must encode
// the pair without collisions and document backend size limits. Components
// are not paths or resource URLs. QueryPartition orders sort keys by UTF-8 bytes.
type KeyValueKey struct {
	PartitionKey string
	SortKey      string
}

// KeyValueVersion is an opaque concurrency token, not a timestamp, counter, or content
// digest. Only return it to the same KeyValueStore and key from which it was obtained.
// The empty value is invalid. Providers must issue a fresh token for every
// successful Create or Replace, even for identical documents and after deletion
// and recreation. Old tokens must never authorize changes to a new incarnation.
type KeyValueVersion string

// KeyValueItem is the application-owned data written to a store. The pair
// (PartitionKey, SortKey) identifies it; Fields contains its typed document.
// Nil and empty Fields both represent an item with no fields.
type KeyValueItem struct {
	PartitionKey string
	SortKey      string
	Fields       KeyValueDocument
}

// Key returns the item's composite identity.
func (item KeyValueItem) Key() KeyValueKey {
	return KeyValueKey{PartitionKey: item.PartitionKey, SortKey: item.SortKey}
}

// KeyValueRecord wraps an atomically read item with its storage metadata.
// The caller owns Item.Fields, including nested maps, lists and byte slices.
type KeyValueRecord struct {
	Item    KeyValueItem
	Version KeyValueVersion

	// LastModifiedAt is the reported modification time in UTC, read atomically
	// with Item and Version. Providers use a native modification timestamp
	// where available; otherwise they generate and persist it atomically on
	// each successful Create or Replace. It must be populated on reads.
	// Writer clocks need not reflect commit time or provide monotonicity,
	// uniqueness or cross-record ordering. Use Version for concurrency.
	LastModifiedAt time.Time
}

// KeyValueQuery requests one bounded page within an exact partition. An empty
// SortKeyPrefix selects every sort key, including the empty key. Nonempty prefixes
// must be valid UTF-8 and match literal bytes (no wildcard or path semantics).
// Results are ordered lexicographically by UTF-8 bytes; Descending reverses that
// order. Use fixed-width encodings when numeric or chronological order is needed.
// PageSize defaults to 100; valid explicit sizes are 1 through 1000.
//
// PageToken is opaque and bound to the physical store, partition, prefix, and
// direction. Only reuse it with that query; PageSize may change between pages.
// Tokens must survive reopening the same store and process restarts, but are not
// authorization grants or secret credentials. They may contain keys: do not log
// or parse them. Invalid or mismatched tokens return ErrInvalidArgument before I/O.
type KeyValueQuery struct {
	PartitionKey  string
	SortKeyPrefix string
	Descending    bool
	PageSize      int
	PageToken     string
}

// KeyValueQueryPage owns its records and all their nested document data.
// A page may contain fewer than PageSize records, including none. Keep querying
// with NextPageToken until it is empty; a token does not promise another record.
// Missing partitions produce an empty successful page, not ErrNotFound.
//
// Reads are strongly consistent, but neither a page nor successive pages form
// an atomic snapshot. Concurrent inserts behind the cursor may be missed, and
// records may be replaced or deleted between pages. Applications must reconcile
// concurrent changes when traversing mutable partitions.
type KeyValueQueryPage struct {
	Records       []KeyValueRecord
	NextPageToken string
}

// KeyValueStore is the provider contract. Each operation must be linearizable for its
// key: completed writes are visible to subsequent reads, and checking a
// condition and performing its mutation is one atomic operation. A successful
// write is acknowledged only after acceptance by the backend's durable write
// mechanism. Reads cannot use an eventually consistent index or replica.
//
// Implementations must be safe for concurrent use and honor context cancellation.
// They must not retain caller-owned documents after a call returns; callers
// must not modify documents during a call. Keys and values must not be logged.
//
// Operation failures must return or wrap *providercontracts.StorageError with a standardized
// kind and the original provider error as Cause. Classify using errors.Is.
// Invalid key components or empty expected versions return ErrInvalidArgument before I/O.
// Invalid documents return ErrInvalidArgument before I/O; see KeyValueDocument.
// Provider-specific size limits must be documented and violations return
// ErrInvalidArgument. Authentication, throttling and other backend errors may
// wrap native errors; the interface does not prescribe a retry policy.
//
// A timeout, cancellation or transport error after a mutation might have been
// sent must include ErrOutcomeUnknown unless the implementation establishes
// whether it committed. Preserve context errors for errors.Is as well. An
// implementation must not disguise an uncertain write as a definitive conflict
// by blindly retrying it. On error, returned records/versions are unusable.
type KeyValueStore interface {
	// Get returns one consistent document and its metadata, or providercontracts.ErrNotFound.
	Get(ctx context.Context, key KeyValueKey) (KeyValueRecord, error)

	// QueryPartition reads an ordered page from the authoritative store, without
	// eventually consistent indexes, field filtering, or cross-partition scans.
	QueryPartition(ctx context.Context, query KeyValueQuery) (KeyValueQueryPage, error)

	// Create stores item only if item.Key() is absent and returns its new version.
	// An existing key returns providercontracts.ErrAlreadyExists without changing it.
	Create(ctx context.Context, item KeyValueItem) (KeyValueVersion, error)

	// Replace stores item only if item.Key() exists with exactly expectedVersion.
	// A missing key or stale version returns providercontracts.ErrConflict.
	Replace(ctx context.Context, item KeyValueItem, expectedVersion KeyValueVersion) (KeyValueVersion, error)

	// Delete removes a record only if its version matches expectedVersion.
	// A missing key or stale version returns providercontracts.ErrConflict; a repeated
	// successful deletion therefore does not silently succeed.
	Delete(ctx context.Context, key KeyValueKey, expectedVersion KeyValueVersion) error
}
