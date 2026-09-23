// Package provider defines how to discover and open existing storage namespaces.
package provider

import (
	"context"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// StoreListOptions requests a page of store names. Zero PageSize uses the
// provider default; negative sizes are invalid. Tokens are opaque, scoped to
// the provider instance and list operation, and must not be parsed or logged.
type StoreListOptions struct {
	PageSize  int
	PageToken string
}

// StoreListPage describes discoverable resources, not a snapshot or an access
// grant. Listing does not guarantee that every resource has the required schema
// or that its contents are accessible. Open validates supported configuration.
// Continue with NextPageToken until it is empty, even if Names is empty.
type StoreListPage struct {
	Names         []string
	NextPageToken string
}

// StorageProvider opens existing stores without provisioning or deleting them.
// Its configuration defines the account/region/namespace scope. Open does not
// require permission to list all stores. Handles are safe for concurrent use.
// Names are provider-specific opaque identifiers. Methods return standardized
// providercontracts.StorageError failures and honor context cancellation.
type StorageProvider interface {
	OpenKeyValueStore(context.Context, string) (kv.KeyValueStore, error)
	OpenBlobStore(context.Context, string) (blob.BlobStore, error)
	ListKeyValueStores(context.Context, StoreListOptions) (StoreListPage, error)
	ListBlobStores(context.Context, StoreListOptions) (StoreListPage, error)
}
