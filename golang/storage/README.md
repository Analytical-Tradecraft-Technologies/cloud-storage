# Go storage library

This directory is reserved for the future caller-facing storage API and helpers.
It does not yet contain a Go module or wrapper implementation.

The independent [provider contracts submodule](providercontracts) contains the provider contracts,
shared models, standardized errors and typed document encoding. The [AWS provider](providers/aws) is a separate submodule depending on those
contracts. The [configured providers module](providers) selects a provider from parsed JSON
and maps application store names to physical resource names. The eventual caller-facing
module can depend on the same contracts while adding convenient operations.

Intended dependency direction:

- Configured providers module → cloud provider modules and provider contracts.
- Cloud provider modules → provider contracts submodule.
- Caller-facing storage module → provider contracts submodule.
- Applications use the JSON loader or a typed cloud constructor to wire the provider into the caller-facing wrapper.

The provider contracts module has no dependency on providers, their SDKs, or the future wrapper.

Run all Go modules' checks from the repository root:

```sh
make fmt
make vet
make test
make build
```
