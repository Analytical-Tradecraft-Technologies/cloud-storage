// Package providers initializes storage providers from parsed JSON configuration.
package providers

import (
	"context"
	"errors"
	"strings"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
)

// FromJSON accepts an object produced by encoding/json, including a subtree of
// a larger document. Only type "aws" is supported. It returns a provider whose
// store names are the configured application aliases, not physical AWS names.
// Configuration is copied; callers may change it after this function returns.
// Initialization loads SDK configuration but does not discover or open resources.
// Open validates a mapped resource on demand; List lists only configured aliases.
func FromJSON(ctx context.Context, object map[string]any) (provider.StorageProvider, error) {
	return fromJSON(ctx, object, newAWS)
}

type initializer func(context.Context, map[string]any) (provider.StorageProvider, error)

func fromJSON(ctx context.Context, object map[string]any, initializeAWS initializer) (provider.StorageProvider, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := knownFields(object, "type", "aws", "key_value_stores", "blob_stores"); err != nil {
		return nil, err
	}
	kind, ok := object["type"].(string)
	if !ok || kind == "" {
		return nil, invalid("type must be a nonempty string")
	}
	if kind != "aws" {
		return nil, &contracts.StorageError{Kind: contracts.ErrUnsupported, Operation: "provider.configure"}
	}
	options, ok := object["aws"].(map[string]any)
	if !ok || options == nil {
		return nil, invalid("aws must be an object")
	}
	kvNames, err := storeNames(object, "key_value_stores")
	if err != nil {
		return nil, err
	}
	blobNames, err := storeNames(object, "blob_stores")
	if err != nil {
		return nil, err
	}
	backend, err := initializeAWS(ctx, options)
	if err != nil {
		return nil, err
	}
	return &configuredProvider{backend: backend, kv: newStoreNames("kv", kvNames), blobs: newStoreNames("blob", blobNames)}, nil
}

func storeNames(object map[string]any, field string) (map[string]string, error) {
	raw, exists := object[field]
	if !exists {
		return map[string]string{}, nil
	}
	values, ok := raw.(map[string]any)
	if !ok || values == nil {
		return nil, invalid("store mappings must be objects")
	}
	result := make(map[string]string, len(values))
	for alias, value := range values {
		name, ok := value.(string)
		if !ok || strings.TrimSpace(alias) == "" || strings.TrimSpace(name) == "" {
			return nil, invalid("store aliases and physical names must be nonempty strings")
		}
		result[alias] = name
	}
	return result, nil
}

func knownFields(object map[string]any, allowed ...string) error {
	for field := range object {
		found := false
		for _, name := range allowed {
			if field == name {
				found = true
				break
			}
		}
		if !found {
			return invalid("unrecognized configuration field")
		}
	}
	return nil
}

func invalid(message string) error {
	return &contracts.StorageError{Kind: contracts.ErrInvalidArgument, Operation: "provider.configure", Cause: errors.New(message)}
}

func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		kind := contracts.ErrCanceled
		if errors.Is(err, context.DeadlineExceeded) {
			kind = contracts.ErrDeadlineExceeded
		}
		return &contracts.StorageError{Kind: kind, Operation: "provider.configure", Cause: err}
	}
	return nil
}
