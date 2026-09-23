package providers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
)

type configuredProvider struct {
	backend   provider.StorageProvider
	kv, blobs namedStores
}

var _ provider.StorageProvider = (*configuredProvider)(nil)

type namedStores struct {
	physical map[string]string
	names    []string
	scope    string
}

func newStoreNames(kind string, names map[string]string) namedStores {
	result := namedStores{physical: names}
	for name := range names {
		result.names = append(result.names, name)
	}
	sort.Strings(result.names)
	// JSON sorts map keys and cannot fail for a map of strings. Bind cursors to
	// the complete mapping and store kind so mismatched cursors fail visibly.
	encoded, _ := json.Marshal(names)
	result.scope = fmt.Sprintf("%s:%x:", kind, sha256.Sum256(encoded))
	return result
}

func (p *configuredProvider) OpenKeyValueStore(ctx context.Context, name string) (kv.KeyValueStore, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	physical, ok := p.kv.physical[name]
	if !ok {
		return nil, missingStore("provider.open_kv")
	}
	return p.backend.OpenKeyValueStore(ctx, physical)
}

func (p *configuredProvider) OpenBlobStore(ctx context.Context, name string) (blob.BlobStore, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	physical, ok := p.blobs.physical[name]
	if !ok {
		return nil, missingStore("provider.open_blob")
	}
	return p.backend.OpenBlobStore(ctx, physical)
}

func missingStore(op string) error {
	return &contracts.StorageError{Kind: contracts.ErrNotFound, Operation: op}
}

func (p *configuredProvider) ListKeyValueStores(ctx context.Context, options provider.StoreListOptions) (provider.StoreListPage, error) {
	return p.kv.list(ctx, options)
}

func (p *configuredProvider) ListBlobStores(ctx context.Context, options provider.StoreListOptions) (provider.StoreListPage, error) {
	return p.blobs.list(ctx, options)
}

func (stores namedStores) list(ctx context.Context, options provider.StoreListOptions) (provider.StoreListPage, error) {
	if err := contextError(ctx); err != nil {
		return provider.StoreListPage{}, err
	}
	if options.PageSize < 0 || options.PageSize > 1000 {
		return provider.StoreListPage{}, invalid("page size must be between 0 and 1000")
	}
	size := options.PageSize
	if size == 0 {
		size = 100
	}
	start := 0
	if options.PageToken != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(options.PageToken)
		if err != nil || !strings.HasPrefix(string(decoded), stores.scope) {
			return provider.StoreListPage{}, invalid("invalid page token")
		}
		start, err = strconv.Atoi(strings.TrimPrefix(string(decoded), stores.scope))
		if err != nil || start <= 0 || start >= len(stores.names) {
			return provider.StoreListPage{}, invalid("invalid page token")
		}
	}
	end := start + min(size, len(stores.names)-start)
	page := provider.StoreListPage{Names: append([]string(nil), stores.names[start:end]...)}
	if end < len(stores.names) {
		page.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(stores.scope + strconv.Itoa(end)))
	}
	return page, nil
}
