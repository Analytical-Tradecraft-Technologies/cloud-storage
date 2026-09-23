# Configured storage providers

Module: `github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providers`

`FromJSON(ctx, object)` accepts a `map[string]any` produced by `encoding/json`
and returns `provider.StorageProvider`. Pass the complete configuration object,
or extract it from a larger parsed document. Only `"aws"` is supported for now.
The provider-specific initializer is unexported; there is no registration or
blank-import requirement. The AWS submodule remains independently usable with
its typed constructors.

## Configuration

```json
{
  "type": "aws",
  "aws": {
    "region": "ap-southeast-2",
    "profile": "development",
    "temp_directory": "/tmp"
  },
  "key_value_stores": {
    "jobs": "production-jobs",
    "leases": "production-leases"
  },
  "blob_stores": {
    "artifacts": "production-artifacts"
  }
}
```

`type` and the `aws` object are required. AWS options are optional strings and
use the existing AWS SDK defaults when omitted or empty. Credentials are loaded
through the IAM credential chain; the JSON does not accept access keys.

Each store map pairs an internal application name with an external resource name.
KV names are DynamoDB table names and blob names are S3 bucket names. The two
alias namespaces are independent. Several aliases may refer to the same resource.
Omitted maps expose no stores of that kind; there is no implicit physical-name
fallback or automatic discovery. Aliases and resource names are case-sensitive,
nonblank strings and are not trimmed or normalized.

For multiple regions or credential configurations, call `FromJSON` separately
for each provider configuration. Tables and buckets must already exist and meet
[the AWS provider's requirements](aws/README.md).

## Using a subtree of parsed JSON

```go
var application map[string]any
if err := json.Unmarshal(configurationBytes, &application); err != nil {
    return err
}
storageConfig, ok := application["storage"].(map[string]any)
if !ok {
    return errors.New("storage must be an object")
}
storage, err := providers.FromJSON(ctx, storageConfig)
if err != nil {
    return err
}
jobs, err := storage.OpenKeyValueStore(ctx, "jobs")
if err != nil {
    return err
}
// jobs implements kv.KeyValueStore and accesses production-jobs.
```

Initialization validates configuration and loads AWS SDK configuration. It does
not list, open, or provision resources. Each `Open` resolves an alias and calls
the backend's resource validation on demand. This avoids requiring permissions
for unused resources at initialization. Returned handles can be reused.

`ListKeyValueStores` and `ListBlobStores` return the configured aliases in sorted
order, without calling AWS. They do not certify that a resource exists or is
accessible. Page size defaults to 100 and is capped at 1000. Treat continuation
tokens as opaque; they are bound to the store kind and complete mapping. They
are not credentials or access grants. Direct AWS-provider listing still returns
physical resource names as before.

The loader copies store mappings before returning, and its immutable state is
safe for concurrent use. Do not mutate the input while initialization is running.
Unknown configuration fields and incorrect JSON types are rejected with
`ErrInvalidArgument`; an unknown provider type returns `ErrUnsupported`; an
unknown alias returns `ErrNotFound`. Backend errors and their underlying causes
are preserved. Duplicate JSON keys have already been resolved by the JSON parser
before this API is called; reject those during parsing if that is required.

## Development

Run `make check` from the repository root to validate all three modules. Run
`go test -race ./...` here for the loader tests. Tests are offline and do not
validate deployed IAM permissions or live AWS resources.

The repository's `go.work` selects sibling modules for development. Module
manifests have no local replacements. External consumption requires the pending
coordinated v0.1.0 release: follow the [release procedure](../RELEASING.md), which
tags the final merged master commit and checks a fresh consumer with `GOWORK=off`.
