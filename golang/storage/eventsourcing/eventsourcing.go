// Package eventsourcing builds typed application state from immutable, versioned
// change batches using only the portable key-value provider contract.
package eventsourcing

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"
	"unicode/utf8"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// EventStreamRevision counts committed batches, not individual changes. Zero is
// the empty stream. Revisions are scoped to a store and stream and never reused.
type EventStreamRevision uint64

// EventChange is one application change, independent of storage batch boundaries.
// Data retains JSON types and number precision; decode it according to Version.
type EventChange struct {
	Version uint64
	Data    json.RawMessage
}

// EventStateBuilder applies changes in their supplied order. A call may combine
// several stored batches; callers must not depend on callback batch boundaries.
// Return an error for application versions or changes that cannot be interpreted.
// Builders must be deterministic and free of external side effects: replay can
// call them repeatedly. The state and change data are owned by this invocation.
type EventStateBuilder[State any] func(context.Context, State, []EventChange) (State, error)

// EventStreamState exposes materialized state without retaining its history.
// Revision is suitable as the expected revision for TryAppend.
type EventStreamState[State any] struct {
	Value    State
	Revision EventStreamRevision
}

// EventAppend is one atomic batch. All changes have ApplicationVersion.
// AppendToken must be nonempty and identify this logical append (for example a
// UUID). To reconcile a lost acknowledgement, repeat the same stream, expected
// revision, token, changes and metadata. Token deduplication is scoped to that
// stream and expected revision; reusing it at a new revision appends again.
// Metadata is optional JSON, e.g. {"service":"worker","version":"1.2.3"}.
// Changes must be nonempty. Each change can be any valid UTF-8 JSON value.
// Callers must not mutate input buffers while an operation is running.
type EventAppend struct {
	AppendToken        string
	ApplicationVersion uint64
	Changes            []json.RawMessage
	Metadata           json.RawMessage
}

// EventStreamStore materializes state and appends history through a generic KV
// store. It never replaces or deletes records. All writers to its reserved
// event/ sort-key namespace must use this protocol; external edits, deletion,
// TTL or lifecycle expiration invalidate its concurrency and replay guarantees.
// It is safe for concurrent use if initial and build are also safe. initial must
// create independent state for each read, including independent maps and slices.
type EventStreamStore[State any] struct {
	storage kv.KeyValueStore
	initial func() State
	build   EventStateBuilder[State]
}

// NewEventStreamStore creates a provider-independent store with a state factory
// and a version-aware builder. It performs no I/O.
func NewEventStreamStore[State any](storage kv.KeyValueStore, initial func() State, build EventStateBuilder[State]) (*EventStreamStore[State], error) {
	if storage == nil || initial == nil || build == nil {
		return nil, eventError("event.new", contracts.ErrInvalidArgument, nil)
	}
	return &EventStreamStore[State]{storage: storage, initial: initial, build: build}, nil
}

// TryAppend atomically creates the batch at expected+1. Only one competing
// batch can win. A stale/future expected revision or a different batch in that
// slot returns ErrConflict. Repeating an already committed identical append
// returns its revision, even if later batches have since been appended.
//
// At most one conditional Create is attempted; there are no automatic mutation
// retries. ErrOutcomeUnknown is preserved. Retry with the same arguments to
// reconcile it, not with a newer expected revision. JSON whitespace is ignored
// during reconciliation; object member ordering is not normalized.
//
// This validates JSON and the storage protocol, not application semantics. Use
// the application's version-aware validation before appending. The builder runs
// only on reads; a builder failure never makes an acknowledged append ambiguous.
func (s *EventStreamStore[State]) TryAppend(ctx context.Context, streamID string, expected EventStreamRevision, appendRequest EventAppend) (EventStreamRevision, error) {
	const op = "event.append"
	if !validStreamID(streamID) || expected == EventStreamRevision(math.MaxUint64) {
		return 0, eventError(op, contracts.ErrInvalidArgument, nil)
	}
	item, err := encodeAppend(streamID, expected+1, appendRequest)
	if err != nil {
		return 0, err
	}
	if err := contextError(ctx, op); err != nil {
		return 0, err
	}
	// Requiring the preceding immutable row prevents fabricated future revisions
	// from creating gaps. No read of a mutable head or cross-row transaction is needed.
	if expected != 0 {
		previous, err := s.storage.Get(ctx, eventKey(streamID, expected))
		if err != nil {
			if errors.Is(err, contracts.ErrNotFound) && !errors.Is(err, contracts.ErrOutcomeUnknown) {
				return 0, eventError(op, contracts.ErrConflict, err)
			}
			return 0, eventError(op, contracts.ErrUnknown, err)
		}
		if _, err := decodeEntry(previous, streamID, expected); err != nil {
			return 0, err
		}
	}
	_, createErr := s.storage.Create(ctx, item)
	if createErr == nil {
		return expected + 1, nil
	}
	// Uncertain writes must never be disguised as a definitive conflict.
	if errors.Is(createErr, contracts.ErrOutcomeUnknown) || !errors.Is(createErr, contracts.ErrAlreadyExists) {
		return 0, eventError(op, contracts.ErrUnknown, createErr)
	}
	stored, readErr := s.storage.Get(ctx, item.Key())
	if readErr != nil {
		return 0, eventError(op, contracts.ErrUnknown, readErr)
	}
	entry, err := decodeEntry(stored, streamID, expected+1)
	if err != nil {
		return 0, err
	}
	if sameAppend(entry, item) {
		return expected + 1, nil
	}
	return 0, eventError(op, contracts.ErrConflict, createErr)
}

// ReadState replays a consistent prefix of the append-only stream. A missing
// stream returns initial() and revision zero. Changes are flattened across rows
// within each page before calling the builder; history is not retained in the
// result. Any read/decoding/builder failure returns no usable partial state.
//
// Concurrent appends may be included. This is not a snapshot at invocation;
// use a context deadline to bound traversal of a continuously growing stream.
func (s *EventStreamStore[State]) ReadState(ctx context.Context, streamID string) (EventStreamState[State], error) {
	const op = "event.read_state"
	var empty EventStreamState[State]
	if !validStreamID(streamID) {
		return empty, eventError(op, contracts.ErrInvalidArgument, nil)
	}
	if err := contextError(ctx, op); err != nil {
		return empty, err
	}
	state := s.initial()
	query := EventHistoryQuery{}
	var revision EventStreamRevision
	seen := make(map[string]struct{})
	for {
		page, err := s.ReadHistory(ctx, streamID, query)
		if err != nil {
			return empty, err
		}
		var changes []EventChange
		for _, entry := range page.Entries {
			for _, change := range entry.Changes {
				changes = append(changes, EventChange{Version: entry.ApplicationVersion, Data: change})
			}
			revision = entry.Revision
		}
		if len(changes) != 0 {
			state, err = s.build(ctx, state, changes)
			if err != nil {
				return empty, eventError(op, contracts.ErrUnknown, err)
			}
		}
		if err := contextError(ctx, op); err != nil {
			return empty, err
		}
		if page.NextPageToken == "" {
			return EventStreamState[State]{Value: state, Revision: revision}, nil
		}
		if _, repeated := seen[page.NextPageToken]; repeated {
			return empty, corruptHistory(op, nil)
		}
		seen[page.NextPageToken] = struct{}{}
		query.PageToken = page.NextPageToken
	}
}

func validStreamID(id string) bool { return id != "" && utf8.ValidString(id) }

func eventError(op string, kind contracts.StorageErrorKind, cause error) error {
	var storageError *contracts.StorageError
	if kind == contracts.ErrUnknown && errors.As(cause, &storageError) && storageError != nil {
		kind = storageError.Kind
	}
	return &contracts.StorageError{Kind: kind, Operation: op, Cause: cause, OutcomeUnknown: errors.Is(cause, contracts.ErrOutcomeUnknown)}
}

func contextError(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		kind := contracts.ErrCanceled
		if errors.Is(err, context.DeadlineExceeded) {
			kind = contracts.ErrDeadlineExceeded
		}
		return eventError(op, kind, err)
	}
	return nil
}

// ErrCorruptHistory indicates malformed records, a missing revision or a broken
// continuation sequence. Never silently skip such records when building state.
var ErrCorruptHistory = errors.New("event sourcing: corrupt history")

func corruptHistory(op string, cause error) error {
	return eventError(op, contracts.ErrUnknown, errors.Join(ErrCorruptHistory, cause))
}

// EventHistoryEntry exposes one stored batch only through the explicit history
// API. RecordedAt is writer-generated UTC time, not ordering or commit time.
// Revision determines order. Metadata and Changes are owned by the caller.
type EventHistoryEntry struct {
	Revision           EventStreamRevision
	RecordedAt         time.Time
	Metadata           json.RawMessage
	AppendToken        string
	ApplicationVersion uint64
	Changes            []json.RawMessage
}
