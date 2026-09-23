# Portable event sourcing

Module: `github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/eventsourcing`

This caller-facing layer depends only on `providercontracts/kv`. It builds typed
application state from immutable change batches and uses conditional `Create` for
optimistic concurrency. It has no AWS SDK, Redis, SQL, transaction or mutable-head
dependency. Supply any provider implementing the KV contract.

## Normal usage: state, not history

```go
type Balance struct { Amount int64 }

events, err := eventsourcing.NewEventStreamStore(
    table, // kv.KeyValueStore opened through the provider interface
    func() Balance { return Balance{} },
    func(ctx context.Context, state Balance, changes []eventsourcing.EventChange) (Balance, error) {
        for _, change := range changes {
            switch change.Version {
            case 1:
                var delta int64
                if err := json.Unmarshal(change.Data, &delta); err != nil {
                    return Balance{}, err
                }
                state.Amount += delta
            default:
                return Balance{}, fmt.Errorf("unsupported balance change version")
            }
        }
        return state, nil
    },
)
if err != nil { return err }

current, err := events.ReadState(ctx, "account/123")
if err != nil { return err }
// current.Value is Balance. current.Revision identifies the last applied batch.

revision, err := events.TryAppend(ctx, "account/123", current.Revision,
    eventsourcing.EventAppend{
        AppendToken: operationID, // a stable UUID, generated once for this append
        ApplicationVersion: 1,
        Changes: []json.RawMessage{json.RawMessage(`10`), json.RawMessage(`-3`)},
        Metadata: json.RawMessage(`{"service":"ledger","version":"1.2.3"}`),
    },
)
if errors.Is(err, providercontracts.ErrOutcomeUnknown) {
    // Reconcile by retrying EXACTLY the same expected revision/token/payload.
    // Do not refresh the revision and blindly append the same business action.
    return err
}
if errors.Is(err, providercontracts.ErrConflict) {
    // Another writer won. Reload state and reconsider the business operation.
    return err
}
if err != nil { return err }
_ = revision
```

`ReadState` supplies flattened changes, each with its application version, to the
builder. One callback can contain changes from several stored rows. Callbacks
follow ascending revision order and then array order within each row; callback
boundaries have no application meaning. JSON stays `json.RawMessage`, preserving
numbers and types without conversion through floating point or strings.

The state factory creates a fresh initial value for every read. A missing stream
returns that initial value and revision zero. Builders must be deterministic,
must support every application version they intend to replay, and must not cause
external effects. Use fresh maps/slices in the factory, and make both callbacks
safe for concurrent reads. They may own and mutate the state/change buffers.

The library validates JSON before writing, but cannot validate arbitrary business
rules. Validate application changes before appending. The builder only runs on
reads, so an append never returns an error merely because a later state rebuild
failed. Reads return no usable partial state if decoding or building fails.

## Storage layout and versions

One partition is one stream. Use a dedicated table or reserve its `event/` sort-key
namespace exclusively for this layer. The partition key is the caller's stream
ID without hashing or rewriting. Batch IDs are sequential revisions encoded as
`event/00000000000000000001`, `event/00000000000000000002`, etc. The fixed width
preserves numeric order on every provider's UTF-8 ordered query interface.

Each row has these logical KV document fields:

| Field | Logical type | Contents |
| --- | --- | --- |
| `recorded_at` | Timestamp | Writer-generated UTC append time |
| `metadata` | Bytes, optional | UTF-8 JSON about the writer/application |
| `event` | Bytes | UTF-8 JSON envelope below |

```json
{
  "format_version": 1,
  "application_version": 3,
  "append_token": "a-stable-application-generated-UUID",
  "changes": [{"set_name":"example"}, {"enabled":true}]
}
```

The library format version and application's change version are separate. All
changes in a batch use that batch's application version; append separate batches
to write different versions. Application version zero is allowed. Future library
formats return `ErrUnsupported`; the library never guesses their interpretation.

Metadata can be any JSON value; omission and explicit JSON `null` are distinct.
Timestamps describe the writer's clock, not commit time, and do not order events.
Revisions order batches; change-array order orders changes within a batch.

These are **logical fields**, not a promise of native database columns. The
current AWS adapter encodes the complete typed KV document in its own binary
attribute. This layer does not bypass the provider to expose DynamoDB attributes.
Provider key/record limits apply to the whole row. Oversized batches fail instead
of being split into potentially partial writes.

## Concurrency and retry guarantees

An append at expected revision N checks that N exists (unless N is zero), then
conditionally creates N+1. The successful create is the commit point. Two writers
with the same expected revision contend for the same row: exactly one distinct
batch wins. Future revisions cannot create holes. There are no staged/orphan rows,
head-pointer writes or cross-record transactions. Normal appends need one create
and, for nonempty streams, one point read of the previous batch.

An already-existing N+1 is read for reconciliation. It succeeds only if the token,
application version, changes and metadata match. The original timestamp remains
unchanged. JSON whitespace is ignored; other representational differences such
as object member ordering are not normalized. Use the same serialized content on
retries. Identical content with a different token is a conflict, not proof of a
retry. Tokens deduplicate only at the same stream/expected revision; they are not
global business-operation IDs or authorization credentials.

There is one mutation attempt per call. Provider errors and uncertain outcomes
remain discoverable through `errors.Is`/`errors.As`, with their original causes.
`ErrOutcomeUnknown` takes precedence over interpreting other classifications as a
definitive failure. A retry with the same arguments reconciles an uncertain write,
even if later batches have since committed. No retry loop is hidden here.

All rows must remain immutable and retained. Do not use TTL, overwrite/delete
history, reuse stream IDs after deletion, or mix in writers that ignore this
protocol. These operations invalidate sequence and retry guarantees. Revision
gaps and malformed rows fail with `ErrCorruptHistory` instead of being skipped;
the library cannot detect arbitrary external removal of an entire stream/tail.

## Explicit history access

For auditing/debugging, use `ReadHistory(ctx, streamID, EventHistoryQuery{})`.
It returns `EventHistoryEntry` batches including revisions, timestamps, metadata,
append tokens and original change arrays. Follow `NextPageToken` until it is empty,
even when a page has no entries. The default is 100 batches per page; explicit
sizes are 1–1000. Keep the stream and page size fixed across pages. Tokens survive
process restarts, are opaque, may contain storage keys, and must not be logged.
Provider tokens remain bound to the underlying physical store.

State reads replay in pages and return a consistent prefix of an immutable stream,
possibly including concurrent appends. They are not a snapshot at invocation.
Use a context deadline for continuously growing streams. Replay costs grow with
history size; this initial layer intentionally has no snapshots, state cache,
subscriptions, cross-stream transactions, deletion or automatic conflict retries.
Pagination bounds row count, not the number of changes inside a stored batch.

## Validation and release

`make check` includes this module. Run `go test -race ./...` here for concurrency,
replay and retry tests against a generic in-memory test adapter. Tests do not
claim live AWS, Azure or GCP validation.

The new module is prepared for `golang/storage/eventsourcing/v0.1.0`. It requires
the contracts module's coordinated v0.1.0 release. The tags are still a post-merge
step; see [RELEASING.md](../RELEASING.md). No local replacements appear in this
module's `go.mod`.
