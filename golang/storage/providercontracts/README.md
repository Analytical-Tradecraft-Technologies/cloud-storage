# Storage provider contracts

Module: `github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts`

This independent Go submodule defines the contracts implemented by storage
providers: shared models, interfaces, error identities and document encoding.
It is the provider abstraction layer, not the final caller-facing API.
It does not connect to a cloud service. The initial packages are:

| Package | Contract |
| --- | --- |
| `providercontracts` | Errors recognized with `errors.Is` |
| `kv` | `KeyValueStore`, `KeyValueKey`, `KeyValueItem`, `KeyValueRecord`, typed fields |
| `blob` | `BlobStore`, `BlobKey`, `BlobReadResult` |
| `provider` | `StorageProvider`, paginated discovery and opening existing stores |

Provider implementations live in separate modules, depend on this provider contracts
module, and accept
provider-specific configuration. Credentials, endpoints, tables, containers,
client ownership and authentication policy do not belong in these contracts.
A configured store represents a single namespace; keys are not authorization.

## KV semantics

`kv.KeyValueStore` provides Get, Create, Replace and Delete.

`Get` returns a `KeyValueRecord` containing the item, modification time and an opaque `kv.KeyValueVersion`. Pass that version back to `Replace`
or `Delete`. A concurrent modification makes the condition fail atomically.
A provider must change the token on every successful write, even when content
is unchanged, and must not reuse tokens when a key is deleted and recreated.
This prevents an old token from updating a different incarnation of a record.

`KeyValueRecord.LastModifiedAt` is the reported modification time in UTC,
returned consistently with the value and version. Azure Table Storage maps its
server-maintained `Timestamp`; Firestore maps `updateTime`. The DynamoDB adapter
will generate a UTC timestamp and persist it atomically with the value and
version on each successful Create or Replace. The field must be populated on
successful reads. Adapter-generated times depend on the writer's clock and are
not guaranteed to be exact commit times, unique, or monotonic. Use the opaque
version token for concurrency; timestamps are descriptive metadata.

Missing reads return `providercontracts.ErrNotFound`; duplicate creates return
`providercontracts.ErrAlreadyExists`; missing or stale conditional mutations return
`providercontracts.ErrConflict`. Empty documents are supported. Partition keys must be nonempty; sort keys may
be empty. Both components must be valid UTF-8, and their pair identifies the
item. Empty expected versions are invalid. Partition queries and ordering are
not part of this initial contract.

The required guarantees are per-key linearizability and durable acknowledgement.
This does not promise survival of arbitrary disasters, cross-region consistency,
or exactly-once external side effects. Providers must document supported service
modes, limits, durability and regional configuration. A provider that cannot
supply the contract must reject that configuration rather than weaken it.

A provider must report `providercontracts.ErrOutcomeUnknown` when a mutation may have
committed but its outcome cannot be established. Preserve any context error too
(for example with `errors.Join`). A caller cannot infer that a timed-out request
failed. Application operation IDs embedded in values can support reconciliation;
matching content alone does not prove which writer committed it.

## Typed items and read metadata

`KeyValueItem` contains application data. `KeyValueRecord` wraps `Item` with
`Version` (the opaque ETag equivalent) and `LastModifiedAt`. Create needs no
fabricated metadata; Replace accepts the item and an explicit expected version.

```go
item := kv.KeyValueItem{
    PartitionKey: "account/123",
    SortKey: "job/456",
    Fields: kv.KeyValueDocument{
        "status": kv.String("pending"),
        "attempts": kv.Int64(0),
        "active": kv.Bool(true),
        "labels": kv.List(kv.String("priority"), kv.String("batch")),
    },
}
version, err := store.Create(ctx, item)
// Handle err before using version.
_ = version
_ = err

record, err := store.Get(ctx, item.Key())
// Handle err before using record.
if err == nil {
    if attempts, ok := record.Item.Fields["attempts"].(kv.KeyValueInt64); ok {
        record.Item.Fields["attempts"] = kv.Int64(attempts.Value() + 1)
        _, err = store.Replace(ctx, record.Item, record.Version)
    }
}
```

Each concrete field type implements `KeyValueFieldValue.Kind()` and provides a
`Value()` method returning its specific Go type. Supported types are null,
bool, string, int64, float64, bytes, UTC timestamp, list and nested document.
Use short constructors to write fields; use type assertions to read known
schemas or a type switch for dynamic schemas. Null has no payload. Integers,
floats and numeric strings remain distinct. Exact decimals are deferred.

The interface representation avoids reserving storage for every possible
payload in every field. Interface boxing can still allocate; this is not a
claim that interfaces outperform inline scalars in every workload. Constructors
and accessors do not deep-copy containers. Store implementations must return
independently owned data and must not retain caller-owned data after calls.

`KeyValueDocument.MarshalBinary` and `UnmarshalBinary` provide a deterministic,
versioned binary fallback that preserves types, int64 precision, float bits and
nanosecond timestamps. Invalid UTF-8, non-finite floats, timestamps outside years
1–9999, cycles and nesting beyond 64 containers are rejected. Nil fields mean
explicit null; missing map entries remain absent. Nil and empty containers are
equivalent. Use the codec explicitly; ordinary JSON marshaling is not the
portable type-preserving format.

Providers may use native attributes only where those preserve these semantics;
otherwise they must encode the affected data. Native backend properties are not
uniform: Azure Table has flat scalar properties, DynamoDB numbers do not retain
integer-versus-float intent, and Firestore restricts directly nested arrays.
Encoding does not remove backend size limits or make encoded fields queryable.

## Standard errors

Providers return or wrap `*providercontracts.StorageError`. It implements `error` and
`Unwrap() error`, retaining the original SDK failure in `Cause`. Local validation
errors can omit the cause. `errors.Is` checks portable kinds; `errors.As` retrieves
either the storage error or the original SDK error through additional wrapping.

```go
return &providercontracts.StorageError{
    Kind:           providercontracts.ErrDeadlineExceeded,
    Operation:      "kv.replace",
    Provider:       "aws",
    Cause:          providerErr,
    OutcomeUnknown: true,
}
```

```go
if errors.Is(err, providercontracts.ErrOutcomeUnknown) {
    // Reconcile the write before deciding whether to repeat it.
}
var detail *providercontracts.StorageError
if errors.As(err, &detail) {
    // Inspect detail.Kind, detail.Operation and detail.Provider.
}
```

Kinds cover missing data, duplicate creates, version conflicts, invalid inputs,
authentication, permissions, throttling, exhausted resources, unavailability,
cancellation, deadlines, unsupported capabilities and unknown failures.
`OutcomeUnknown` is independent of the primary kind: a timed-out mutation can
match both `ErrDeadlineExceeded` and `ErrOutcomeUnknown`. The latter can also be
used as the primary kind when no more specific failure is known. No kind alone
promises that retrying is safe. Use `errors.Join(providerErr, ctx.Err())` as the
cause when both SDK and context identities need preserving; standard context
errors match only when present in the cause chain.

The default error message includes static provider/operation labels and the
classification. It omits provider error text because that may contain keys or
sensitive request details. The full cause remains available for explicit
inspection. Providers must classify using SDK codes/types, not message parsing;
unrecognized failures use `ErrUnknown` with the original cause.

## Multiple stores

`provider.StorageProvider` opens an existing KV or blob store by name and offers
separate paginated `ListKeyValueStores` and `ListBlobStores` operations. Provider
configuration determines the account and region scope. Listing resources does
not grant access or certify that their schemas are supported. Opening a known
store does not require permission for account-wide discovery.

Stores are namespaces, not individual records or objects. Provisioning stores,
listing records/objects within a store, and a final caller-facing wrapper are
separate concerns and are not added by this interface. The initial
[AWS implementation](../providers/aws) maps stores to existing tables and buckets.

## Blob semantics

`blob.BlobStore` provides Create, Open and Delete; Open returns a `blob.BlobReadResult`.

Use unique keys or application-selected content hashes for immutable objects.
`Create` never overwrites, including when the same bytes are supplied twice.
The caller provides the exact size and retains ownership of the input reader.
`Open` returns a streaming body that the caller closes; read errors can occur
well after `Open` succeeds. Content hashes, media types and application metadata
can live in the application's KV record; this interface does not compute them.

To publish a blob reference in KV, create the object first and conditionally
publish its key afterward. Those operations are not atomic together. Applications
must account for orphaned objects and ensure lifecycle policies do not remove
objects while their references remain in use.

`Delete(ctx, key)` removes the currently visible blob. Missing blobs are a
successful no-op. After success, Open returns `ErrNotFound` unless another Create
publishes that key again. Existing readers may fail after deletion. Deletion is
unconditional: a retry can remove a replacement if the key was reused, so use
unique keys where that race matters. Unconfirmed deletions use
`ErrOutcomeUnknown`; retention and permission failures must surface as errors.
Deletion makes the blob unavailable through this contract; it does not promise
erasure of historical versions, snapshots or backups.

## Backend mappings

| Contract | AWS | Azure | Google Cloud |
| --- | --- | --- | --- |
| KV conditional replacement | DynamoDB conditional write on an adapter-managed revision | Table Storage ETag / If-Match | Firestore updateTime precondition |
| KV create | Conditional put if absent | Insert entity | Create document |
| Blob create | S3 If-None-Match: * | Blob Storage If-None-Match: * | Cloud Storage ifGenerationMatch=0 |

Providers must use authoritative, strongly consistent KV reads. Tokens are
opaque and bound to the store/key; an adapter may need an incarnation component
to enforce the no-token-reuse rule. Arbitrary backend conditional expressions
are not exposed. There are no partition, transaction or query APIs yet, so the
contract does not assume DynamoDB's cross-key transaction capabilities.

References:
- [DynamoDB conditional operations](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)
- [Azure Table conditional updates](https://learn.microsoft.com/en-us/rest/api/storageservices/update-entity2)
- [Firestore preconditions](https://docs.cloud.google.com/firestore/docs/reference/rest/v1/Precondition)
- [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [Azure Blob conditional headers](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations)
- [Cloud Storage preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions)

## Deliberately deferred

Additional provider implementations, in-memory stores, conformance suites, automatic
retries, transactions, listing, TTL, configuration
loaders and metrics wrappers. Add these when a concrete caller requires them.
Provider implementations should bring shared behavioral tests for concurrency,
stale tokens, delete/recreate races, uncertain writes and stream failures.

Check the contracts, codec and documentation with:

```sh
go test ./...
go vet ./...
go doc ./kv
go doc ./blob
```
