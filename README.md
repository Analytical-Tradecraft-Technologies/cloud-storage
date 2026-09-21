# cloud-storage

Portable storage contracts and, in future, cloud provider implementations and a
caller-facing API.

The Go library lives under [golang/storage](golang/storage). Its first independent
Go submodule is [golang/storage/providercontracts](golang/storage/providercontracts), the abstraction
layer that provider implementations implement. It contains KV and immutable blob
contracts, shared models, errors and typed document encoding, with no cloud SDK
dependencies. The caller-facing wrapper and provider modules are not implemented
yet.
