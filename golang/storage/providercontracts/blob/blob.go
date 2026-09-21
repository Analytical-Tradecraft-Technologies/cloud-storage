// Package blob defines a minimal immutable object-store contract for large
// payloads. Providers configure the bucket/container and namespace separately.
package blob

import (
	"context"
	"io"
)

// BlobKey identifies an immutable object within a configured BlobStore. It is nonempty,
// case-sensitive valid UTF-8, not a public URL or trusted filesystem path.
// Providers must map keys without collisions and document supported lengths.
type BlobKey string

// BlobReadResult is an opened object. The caller must close Body, including after a
// failed read. Size is its nonnegative byte length. Read errors, including
// errors after Open succeeds, are reported by Body.Read. The request context
// remains in effect until Body is closed; consuming the body can still fail.
type BlobReadResult struct {
	Body io.ReadCloser
	Size int64
}

// BlobStore publishes immutable objects, streams their content and deletes them.
// Immutability prevents overwriting content; keys may be reused after deletion. It is safe for
// concurrent use. An object is visible only after its complete content has
// been published; subsequent Open calls must see a successfully created object.
// No transaction with a kv.KeyValueStore is implied.
//
// Operation failures return or wrap *providercontracts.StorageError, preserving provider
// errors as Cause and supporting errors.Is with storage kinds. Invalid keys, nil
// bodies, negative sizes and documented provider limit violations return
// ErrInvalidArgument. Keys and contents must not be logged. For uncertain
// mutation outcomes, use ErrOutcomeUnknown, preserving context cancellation
// errors as well. Implementations must not blindly retry uncertain creates
// and report the resulting AlreadyExists as a definitive initial failure.
//
// The initial interface deliberately excludes replacement, metadata, listing,
// signed URLs and range reads. Retention/lifecycle settings are deployment-owned.
type BlobStore interface {
	// Create publishes exactly size bytes at key only if no object exists.
	// Existing objects return providercontracts.ErrAlreadyExists, even for identical
	// content. Implementations never overwrite an existing object.
	//
	// body must produce exactly size bytes followed by EOF. A length mismatch
	// returns ErrInvalidArgument and must not publish the object. Providers may
	// stage uploads to enforce this; abandoned staging data is their cleanup
	// responsibility. The caller owns and closes body, and must not read it
	// concurrently. Implementations finish using it before returning.
	Create(ctx context.Context, key BlobKey, body io.Reader, size int64) error

	// Open returns a non-nil body and size, or providercontracts.ErrNotFound. On error,
	// the returned BlobReadResult is unusable and the provider closes any opened body.
	Open(ctx context.Context, key BlobKey) (BlobReadResult, error)

	// Delete removes the currently visible blob at key. A missing blob is a
	// successful no-op. After success, subsequent Open calls return ErrNotFound
	// unless a concurrent or later Create publishes another blob at that key.
	// Deletion does not guarantee that already opened readers can finish.
	//
	// This is unconditional deletion by key, not by object version. A retry can
	// delete a newly created blob if the key was reused; use unique keys when
	// that race is unacceptable. Report ErrOutcomeUnknown if deletion may have
	// committed but cannot be confirmed. Retention or authorization failures
	// must be reported, not treated as success.
	//
	// Success means the blob is no longer accessible through Open. It does not
	// promise permanent erasure of historical versions, snapshots or backups;
	// their retention and cleanup remain backend/deployment concerns.
	Delete(ctx context.Context, key BlobKey) error
}
