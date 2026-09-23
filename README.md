# cloud-storage

Portable storage contracts and cloud provider implementations.

The Go library lives under [golang/storage](golang/storage):

- [Provider contracts](golang/storage/providercontracts): KV, blob and store
  discovery interfaces, typed models, standardized errors and document encoding.
- [AWS provider](golang/storage/providers/aws): DynamoDB and S3 with IAM signing.

The caller-facing wrapper remains a future module. Provider contracts have no
cloud SDK dependencies; each provider is an independent Go submodule.

From the repository root, run `make fmt`, `make vet`, `make test` and
`make build` to check every Go module. The shared GitHub Actions workflow does the same.
