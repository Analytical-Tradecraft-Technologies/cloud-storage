# AWS storage provider

Module: `github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providers/aws`

Package `awsprovider` implements the KV, blob and provider-discovery contracts
with AWS SDK for Go v2. It does not provision resources. No code here creates
buckets/tables, changes IAM policies or modifies infrastructure configuration.

## Multiple stores and IAM

One `AWSStorageProvider` represents one region and credential configuration.
Each KV handle targets one existing DynamoDB table; each blob handle targets one
existing general-purpose S3 bucket. Handles share concurrency-safe SDK clients.
Use separate providers for different regions or assumed roles/accounts.

```go
p, err := awsprovider.New(ctx, awsprovider.AWSProviderConfig{
    Region: "ap-southeast-2",
    // Profile: "development", // Optional local AWS profile.
})
if err != nil {
    return err
}

jobs, err := p.OpenKeyValueStore(ctx, "application-jobs")
if err != nil {
    return err
}
archives, err := p.OpenBlobStore(ctx, "application-archives")
if err != nil {
    return err
}
// jobs and archives implement the provider contracts.
_ = jobs
_ = archives
```

`New` uses `config.LoadDefaultConfig`: environment/shared profiles and renewable
IAM credentials from workload roles, web identity, ECS or EC2 as configured by
the SDK. IAM Identity Center profiles and SDK-supported credential processes can
also be used. No access keys are embedded or persisted by this library.
`NewFromConfig(aws.Config)` accepts existing SDK configuration, including an
assumed-role credentials provider. Anonymous credentials are rejected. Each
request is signed by the SDK. SDK request logging is disabled.

Discovery is optional and paginated:

```go
options := provider.StoreListOptions{PageSize: 100}
for {
    page, err := p.ListKeyValueStores(ctx, options)
    if err != nil {
        return err
    }
    for _, name := range page.Names {
        // Select a table, then call OpenKeyValueStore to validate its schema.
        _ = name
    }
    if page.NextPageToken == "" {
        break
    }
    options.PageToken = page.NextPageToken
}
```

Use `ListBlobStores` the same way. DynamoDB listing covers tables in the current
account/region, with page sizes 1–100. S3 listing covers account-owned
general-purpose buckets in this region, with page sizes 1–10,000. The default
page size is 100. These are discovery results, not an authorization list or a
snapshot. Cross-account buckets may be opened by name when IAM permits, but do
not appear in account-owned bucket discovery. Tokens stay opaque and should not
be reused across providers or listing methods.

## DynamoDB layout and consistency

A store requires an ACTIVE, single-region table with these primary keys:

| Attribute | DynamoDB type | Role |
| --- | --- | --- |
| `pk` | String | Partition key |
| `sk` | String | Sort key |

`OpenKeyValueStore` calls DescribeTable and rejects incompatible schemas and
global tables. Data operations use the validated table ARN, so rotating credentials
cannot redirect a handle to an equally named table in another account. Keep the
table single-region while using the handle; reopen handles after deleting and
recreating a table. Table identity is validated at open, not before every call. All writers
must use this adapter's format and concurrency protocol; external unconditional
writes or lifecycle deletion can violate its guarantees.

The adapter owns the table's record format:

- `pk` and `sk`: logical key prefixed with `s`, allowing an empty logical sort key.
- `_cs_document`: the shared KVD1 binary document encoding.
- `_cs_version`: a fresh cryptographically random 256-bit revision per write.
- `_cs_modified`: writer-generated UTC RFC3339Nano modification time.

Logical partition keys allow 1–2,047 UTF-8 bytes, and sort keys 0–1,023 bytes.
The 400 KiB DynamoDB item limit includes keys, metadata, attribute names and
encoded fields; oversized records are rejected before I/O. Existing arbitrary
DynamoDB items are not imported automatically. Corrupt stored envelopes return
`ErrUnknown`, not a caller validation error.

The binary document retains exact integers, float intent, nulls, nested lists,
bytes and nanosecond timestamps. This initial implementation favors a small,
stable encoding over native field indexing: fields cannot be queried or updated
as DynamoDB attributes. The contract currently has no field queries or patches.

Reads use `ConsistentRead`. Create uses `attribute_not_exists(pk)`; Replace and
Delete atomically compare `_cs_version`. Identical writes get new revisions;
delete/recreate does not deliberately reuse a revision. Timestamps are generated
by the writer and are descriptive, not the concurrency condition.

## Partition queries

`QueryPartition` uses a strongly consistent DynamoDB Query on the base table,
with an exact encoded partition and optional literal sort-key prefix. It never
uses Scan, secondary indexes, post-read filters, or automatic pagination. UTF-8
byte order is preserved by the constant key prefix. `Descending` reverses the
order. Page sizes default to 100 and are capped at 1000 records; DynamoDB's 1 MiB
response bound can end a page earlier. No cross-record or multi-page snapshot is
promised. See the [portable query example](../../providercontracts/README.md#partition-queries).

Continuation tokens are versioned, bounded to 16 KiB, and bind the table ARN
(account/region/name), table incarnation ID, partition, prefix and direction.
They work after reopening the same table or restarting a process; changing page
size is permitted. Reopening a recreated table rejects its predecessor's tokens.
Tokens contain keys and must not be logged. They are opaque continuation state,
not signed authorization grants: applications must authorize every query.
Malformed/mismatched tokens fail before I/O. Corrupt or out-of-order backend
records fail the entire page with `ErrUnknown`; no partial result is returned.

## S3 behavior

Only general-purpose buckets in the provider's region are supported. Opening a
bucket uses HeadBucket to check access and region. Directory buckets, access
point ARNs, automatic cross-region routing and resource provisioning are not
supported. Blob keys are nonempty UTF-8 strings of at most 1,024 bytes.

Create uses PutObject with `If-None-Match: *`. This initial implementation supports
up to 5,000,000,000 bytes per blob. To verify exact size and EOF before publishing,
it spools at most declared size plus one byte to a private mode-0600 temporary
file, using bounded copy buffers rather than holding the entire blob in memory.
Set `AWSProviderConfig.TempDirectory` to choose the staging location. Staging files
are closed and removal is attempted on all return paths. Process termination or
filesystem failures can leave files behind; the deployment must manage its
private staging directory. The input reader stays caller-owned and is not closed.
Cancellation is checked between reads; arbitrary blocking caller-provided readers
must themselves honor cancellation if prompt interruption is required.

Open streams the object and wraps later read/close failures in standardized
errors. Close the returned body. Delete removes the currently visible object;
a missing object is a no-op. With S3 versioning this can create a delete marker,
so it does not erase historical versions or backups. Retention/IAM failures are
reported. As deletion is by key, a retry may delete a recreated object.

## Errors and retry policy

Errors wrap the original SDK error and expose portable classifications through
`errors.Is` and SDK types through `errors.As`. No error message includes the raw
provider cause. SDK retries are disabled for this initial provider, including
reads. This prevents a lost successful mutation response followed by a retry
from turning into a misleading conflict. Request cancellation before submission
has a known outcome; transport/server failures after entering a mutation call
are conservatively marked `OutcomeUnknown`. Reconcile before deciding to retry.
The adapter does not add automatic idempotency reconciliation.

## IAM permissions

Grant only the operations each application needs:

| Usage | Permissions |
| --- | --- |
| Open KV store | `dynamodb:DescribeTable` on the table |
| KV data | `dynamodb:GetItem`, `dynamodb:Query`, `dynamodb:PutItem`, `dynamodb:DeleteItem` on the table |
| Discover KV stores | `dynamodb:ListTables` on `*` |
| Open blob store | `s3:ListBucket` on the bucket (HeadBucket) |
| Blob data | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on its objects |
| Discover blob stores | `s3:ListAllMyBuckets` on `*` |

Bucket policies, organization policies, KMS encryption and other deployment
settings may require additional permissions. Discovery permissions are not
needed when opening a known store. Provisioning permissions are never needed.

## Development and release

Local development uses the repository's `go.work`; module manifests contain real
version requirements and no local replacements. The coordinated v0.1.0 release
is pending: see the [release procedure](../../RELEASING.md) for the required tags
on merged master and the independent consumer check. Workspace checks alone do
not prove that those versions have been published.

Run `go test -race ./...`, `go vet ./...` and `go build ./...` in this module, or
use `make check` from the repository root. Tests use in-process
SDK fakes and an HTTP transport that checks IAM signing and one-attempt writes;
they need no AWS account, credentials, network or live resources. They do not
prove deployed IAM permissions or real AWS service behavior.

References:
- [AWS SDK configuration and credential chain](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-gosdk.html)
- [DynamoDB Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html)
- [DynamoDB conditional PutItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html)
- [DynamoDB limits](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html)
- [S3 conditional PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
- [Regional paginated bucket discovery](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html)
