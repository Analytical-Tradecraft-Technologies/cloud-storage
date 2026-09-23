package providers

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
)

type fakeProvider struct {
	provider.StorageProvider // Listing must never reach the backend.
	tables, buckets          []string
	cause                    error
}

func (f *fakeProvider) OpenKeyValueStore(_ context.Context, name string) (kv.KeyValueStore, error) {
	f.tables = append(f.tables, name)
	return nil, f.cause
}
func (f *fakeProvider) OpenBlobStore(_ context.Context, name string) (blob.BlobStore, error) {
	f.buckets = append(f.buckets, name)
	return nil, f.cause
}
func parsed(t *testing.T, text string) map[string]any {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal([]byte(text), &object); err != nil {
		t.Fatal(err)
	}
	return object
}
func loadFake(t *testing.T, object map[string]any, backend *fakeProvider) provider.StorageProvider {
	t.Helper()
	p, err := fromJSON(context.Background(), object, func(_ context.Context, options map[string]any) (provider.StorageProvider, error) {
		if _, err := awsConfig(options); err != nil {
			return nil, err
		}
		return backend, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestParsedSubtreeAndMultipleStores(t *testing.T) {
	root := parsed(t, `{"unrelated":42,"storage":{"type":"aws","aws":{"region":"ap-southeast-2"},"key_value_stores":{"jobs":"prod-jobs","leases":"prod-leases"},"blob_stores":{"jobs":"prod-artifacts"}}}`)
	object := root["storage"].(map[string]any)
	backend := &fakeProvider{}
	p := loadFake(t, object, backend)
	// The caller's JSON may be discarded or changed after loading.
	object["key_value_stores"].(map[string]any)["jobs"] = "changed"
	for _, name := range []string{"jobs", "leases"} {
		if _, err := p.OpenKeyValueStore(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.OpenBlobStore(context.Background(), "jobs"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(backend.tables, []string{"prod-jobs", "prod-leases"}) || !reflect.DeepEqual(backend.buckets, []string{"prod-artifacts"}) {
		t.Fatalf("incorrect routing: %+v", backend)
	}
	if _, err := p.OpenKeyValueStore(context.Background(), "prod-jobs"); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("physical-name fallback: %v", err)
	}
	if len(backend.tables) != 2 {
		t.Fatal("unknown alias reached backend")
	}
	cause := &contracts.StorageError{Kind: contracts.ErrPermissionDenied, Provider: "aws"}
	backend.cause = cause
	if _, err := p.OpenBlobStore(context.Background(), "jobs"); err != cause {
		t.Fatal("provider error was not preserved")
	}
}

func TestConfigurationValidationBeforeInitialization(t *testing.T) {
	for _, text := range []string{
		`null`, `{}`, `{"type":1}`, `{"type":"aws"}`, `{"type":"aws","aws":null}`,
		`{"type":"aws","aws":{},"unknown":true}`,
		`{"type":"aws","aws":{},"key_value_stores":null}`,
		`{"type":"aws","aws":{},"key_value_stores":[]}`,
		`{"type":"aws","aws":{},"key_value_stores":{"a":3}}`,
		`{"type":"aws","aws":{},"blob_stores":{" ":"bucket"}}`,
		`{"type":"aws","aws":{},"blob_stores":{"a":" "}}`,
	} {
		t.Run(text, func(t *testing.T) {
			called := false
			_, err := fromJSON(context.Background(), parsed(t, text), func(context.Context, map[string]any) (provider.StorageProvider, error) {
				called = true
				return nil, nil
			})
			if !errors.Is(err, contracts.ErrInvalidArgument) || called {
				t.Fatalf("err=%v initialized=%v", err, called)
			}
		})
	}
	for _, kind := range []string{"azure", "gcp", "AWS"} {
		_, err := FromJSON(context.Background(), map[string]any{"type": kind})
		if !errors.Is(err, contracts.ErrUnsupported) {
			t.Fatalf("%s: %v", kind, err)
		}
	}
}

func TestAWSConfig(t *testing.T) {
	cfg, err := awsConfig(parsed(t, `{"region":"ap-southeast-2","profile":"development","temp_directory":"/tmp/staging"}`))
	if err != nil || cfg.Region != "ap-southeast-2" || cfg.Profile != "development" || cfg.TempDirectory != "/tmp/staging" {
		t.Fatalf("%+v %v", cfg, err)
	}
	for _, text := range []string{`{"region":2}`, `{"profile":null}`, `{"temp_directory":false}`, `{"regoin":"x"}`, `{"access_key":"secret"}`} {
		if _, err := awsConfig(parsed(t, text)); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatalf("%s: %v", text, err)
		}
	}
	// Exercise the public dispatch and real SDK configuration loading offline.
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/absent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/absent")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	p, err := FromJSON(context.Background(), parsed(t, `{"type":"aws","aws":{"region":"ap-southeast-2"}}`))
	if err != nil {
		t.Fatal(err)
	}
	page, err := p.ListKeyValueStores(context.Background(), provider.StoreListOptions{})
	if err != nil || len(page.Names) != 0 {
		t.Fatalf("%+v %v", page, err)
	}
}

func TestPaginationAndIsolation(t *testing.T) {
	p := loadFake(t, parsed(t, `{"type":"aws","aws":{},"key_value_stores":{"z":"table-z","a":"table-a","m":"table-m"},"blob_stores":{"a":"bucket-a","m":"bucket-m","z":"bucket-z"}}`), &fakeProvider{})
	first, err := p.ListKeyValueStores(context.Background(), provider.StoreListOptions{PageSize: 1})
	if err != nil || !reflect.DeepEqual(first.Names, []string{"a"}) || first.NextPageToken == "" {
		t.Fatalf("%+v %v", first, err)
	}
	first.Names[0] = "modified"
	next, err := p.ListKeyValueStores(context.Background(), provider.StoreListOptions{PageSize: 2, PageToken: first.NextPageToken})
	if err != nil || !reflect.DeepEqual(next.Names, []string{"m", "z"}) || next.NextPageToken != "" {
		t.Fatalf("%+v %v", next, err)
	}
	all, err := p.ListKeyValueStores(context.Background(), provider.StoreListOptions{})
	if err != nil || !reflect.DeepEqual(all.Names, []string{"a", "m", "z"}) {
		t.Fatalf("%+v %v", all, err)
	}
	if _, err := p.ListBlobStores(context.Background(), provider.StoreListOptions{PageToken: first.NextPageToken}); !errors.Is(err, contracts.ErrInvalidArgument) {
		t.Fatalf("cross-kind cursor: %v", err)
	}
	other := loadFake(t, parsed(t, `{"type":"aws","aws":{},"key_value_stores":{"a":"different","m":"table-m","z":"table-z"}}`), &fakeProvider{})
	if _, err := other.ListKeyValueStores(context.Background(), provider.StoreListOptions{PageToken: first.NextPageToken}); !errors.Is(err, contracts.ErrInvalidArgument) {
		t.Fatalf("cross-mapping cursor: %v", err)
	}
	for _, options := range []provider.StoreListOptions{{PageSize: -1}, {PageSize: 1001}, {PageToken: "invalid"}} {
		if _, err := p.ListKeyValueStores(context.Background(), options); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatalf("%+v: %v", options, err)
		}
	}
}

func TestCancellationAndInitializationError(t *testing.T) {
	object := parsed(t, `{"type":"aws","aws":{},"key_value_stores":{"jobs":"prod-jobs"}}`)
	backend := &fakeProvider{}
	p := loadFake(t, object, backend)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := FromJSON(ctx, object); !errors.Is(err, context.Canceled) || !errors.Is(err, contracts.ErrCanceled) {
		t.Fatalf("%v", err)
	}
	if _, err := p.OpenKeyValueStore(ctx, "jobs"); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if _, err := p.ListBlobStores(ctx, provider.StoreListOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if len(backend.tables) != 0 {
		t.Fatal("canceled open reached backend")
	}
	cause := errors.New("configuration load failure")
	result, err := fromJSON(context.Background(), object, func(context.Context, map[string]any) (provider.StorageProvider, error) { return nil, cause })
	if result != nil || err != cause {
		t.Fatalf("%v %v", result, err)
	}
}
