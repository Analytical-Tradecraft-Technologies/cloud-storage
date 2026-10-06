package awsprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func compatibleTable() *ddbtypes.TableDescription {
	return &ddbtypes.TableDescription{
		TableStatus:          ddbtypes.TableStatusActive,
		TableId:              aws.String("test-table-id"),
		TableArn:             aws.String("arn:aws:dynamodb:ap-southeast-2:123456789012:table/test"),
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}, {AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
	}
}

func TestOpenMultipleStoresWithoutListing(t *testing.T) {
	var wantARN string
	d := &fakeDynamo{describe: func(in *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
		table := compatibleTable()
		table.TableArn = aws.String("arn:aws:dynamodb:ap-southeast-2:123456789012:table/" + *in.TableName)
		wantARN = *table.TableArn
		return &dynamodb.DescribeTableOutput{Table: table}, nil
	}, get: func(in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if aws.ToString(in.TableName) != wantARN {
			t.Fatal("read must target the validated account/region/table ARN")
		}
		return &dynamodb.GetItemOutput{}, nil
	}, query: func(in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if aws.ToString(in.TableName) != wantARN {
			t.Fatal("query must target the validated account/region/table ARN")
		}
		return &dynamodb.QueryOutput{}, nil
	}}
	b := &fakeS3{head: func(*s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
		return &s3.HeadBucketOutput{BucketRegion: aws.String("ap-southeast-2")}, nil
	}}
	p := &AWSStorageProvider{dynamo: d, s3: b, region: "ap-southeast-2"}
	for _, name := range []string{"store-a", "store-b"} {
		store, err := p.OpenKeyValueStore(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		if store.(*dynamoStore).table != wantARN {
			t.Fatal("wrong table")
		}
		if _, err := store.Get(context.Background(), kv.KeyValueKey{PartitionKey: "account"}); !errors.Is(err, contracts.ErrNotFound) {
			t.Fatal(err)
		}
		if _, err := store.QueryPartition(context.Background(), kv.KeyValueQuery{PartitionKey: "account"}); err != nil {
			t.Fatal(err)
		}
		blob, err := p.OpenBlobStore(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		if blob.(*s3Store).bucket != name {
			t.Fatal("wrong bucket")
		}
	}
	// No list handler exists: opening never calls either discovery operation.
}

func TestRejectIncompatibleStores(t *testing.T) {
	for _, mutate := range []func(*ddbtypes.TableDescription){
		func(d *ddbtypes.TableDescription) { d.KeySchema = d.KeySchema[:1] },
		func(d *ddbtypes.TableDescription) {
			d.AttributeDefinitions[0].AttributeType = ddbtypes.ScalarAttributeTypeN
		},
		func(d *ddbtypes.TableDescription) {
			d.Replicas = []ddbtypes.ReplicaDescription{{RegionName: aws.String("us-east-1")}}
		},
	} {
		table := compatibleTable()
		mutate(table)
		p := &AWSStorageProvider{dynamo: &fakeDynamo{describe: func(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: table}, nil
		}}}
		if _, err := p.OpenKeyValueStore(context.Background(), "table"); !errors.Is(err, contracts.ErrUnsupported) {
			t.Fatal(err)
		}
	}
	p := &AWSStorageProvider{region: "ap-southeast-2", s3: &fakeS3{head: func(*s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
		return &s3.HeadBucketOutput{BucketRegion: aws.String("us-east-1")}, nil
	}}}
	if _, err := p.OpenBlobStore(context.Background(), "bucket"); !errors.Is(err, contracts.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := p.OpenBlobStore(context.Background(), "bucket--az1--x-s3"); !errors.Is(err, contracts.ErrUnsupported) {
		t.Fatal(err)
	}
}

func TestStoreDiscoveryPaginationAndRegion(t *testing.T) {
	p := &AWSStorageProvider{region: "ap-southeast-2",
		dynamo: &fakeDynamo{list: func(in *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
			if aws.ToInt32(in.Limit) != 10 {
				t.Fatal("wrong table page size")
			}
			if in.ExclusiveStartTableName == nil {
				return &dynamodb.ListTablesOutput{TableNames: []string{"table-a"}, LastEvaluatedTableName: aws.String("table-a")}, nil
			}
			if *in.ExclusiveStartTableName != "table-a" {
				t.Fatal("lost table cursor")
			}
			return &dynamodb.ListTablesOutput{TableNames: []string{"table-b"}}, nil
		}},
		s3: &fakeS3{list: func(in *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
			if aws.ToString(in.BucketRegion) != "ap-southeast-2" || aws.ToInt32(in.MaxBuckets) != 10 {
				t.Fatal("wrong bucket region/page size")
			}
			if in.ContinuationToken == nil {
				return &s3.ListBucketsOutput{Buckets: []s3types.Bucket{{Name: aws.String("bucket-a")}}, ContinuationToken: aws.String("opaque-token")}, nil
			}
			if *in.ContinuationToken != "opaque-token" {
				t.Fatal("lost bucket cursor")
			}
			return &s3.ListBucketsOutput{Buckets: []s3types.Bucket{{Name: aws.String("bucket-b")}}}, nil
		}}}
	for _, list := range []func(context.Context, provider.StoreListOptions) (provider.StoreListPage, error){p.ListKeyValueStores, p.ListBlobStores} {
		first, err := list(context.Background(), provider.StoreListOptions{PageSize: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(first.Names) != 1 || first.NextPageToken == "" {
			t.Fatal("missing first page")
		}
		second, err := list(context.Background(), provider.StoreListOptions{PageSize: 10, PageToken: first.NextPageToken})
		if err != nil {
			t.Fatal(err)
		}
		if len(second.Names) != 1 || second.NextPageToken != "" || first.Names[0] == second.Names[0] {
			t.Fatal("bad second page")
		}
		if _, err := list(context.Background(), provider.StoreListOptions{PageSize: -1}); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatal(err)
		}
	}
}

type httpClientFunc func(*http.Request) (*http.Response, error)

func (f httpClientFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func TestSDKSignsRequestsAndDoesNotRetryMutations(t *testing.T) {
	for _, service := range []string{"dynamodb", "s3"} {
		t.Run(service, func(t *testing.T) {
			calls := 0
			p, err := NewFromConfig(aws.Config{Region: "ap-southeast-2", Credentials: credentials.NewStaticCredentialsProvider("test-access", "test-secret", "test-session"), RetryMaxAttempts: 5, HTTPClient: httpClientFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") || !strings.Contains(r.Header.Get("Authorization"), "/ap-southeast-2/"+service+"/aws4_request") || r.Header.Get("X-Amz-Security-Token") != "test-session" {
					t.Fatal("missing IAM request signature")
				}
				body := `{"__type":"InternalServerError","message":"private"}`
				contentType := "application/x-amz-json-1.0"
				if service == "s3" {
					body = `<Error><Code>InternalError</Code><Message>private</Message></Error>`
					contentType = "application/xml"
				}
				return &http.Response{StatusCode: 500, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			if service == "dynamodb" {
				_, err = (&dynamoStore{client: p.dynamo, table: "table"}).Create(context.Background(), kv.KeyValueItem{PartitionKey: "p"})
			} else {
				err = (&s3Store{client: p.s3, bucket: "bucket", tempDirectory: t.TempDir()}).Create(context.Background(), "key", strings.NewReader("body"), 4)
			}
			if calls != 1 {
				t.Fatalf("mutation sent %d times", calls)
			}
			if !errors.Is(err, contracts.ErrOutcomeUnknown) || !errors.Is(err, contracts.ErrUnavailable) {
				t.Fatalf("bad error: %v", err)
			}
		})
	}
}

func TestSDKConfigurationRequiresIAM(t *testing.T) {
	for _, cfg := range []aws.Config{{}, {Region: "ap-southeast-2"}, {Region: "ap-southeast-2", Credentials: aws.AnonymousCredentials{}}} {
		if _, err := NewFromConfig(cfg); err == nil {
			t.Fatal("invalid IAM configuration accepted")
		}
	}
}

func TestTransportDeadlinePreservesOutcomeAndCause(t *testing.T) {
	cause := errors.New("transport lost")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := wrapError(ctx, "kv.create", cause, true)
	if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) || !errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatal(err)
	}
}

func TestMRSCOptInAndValidation(t *testing.T) {
	for _, allow := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			mutate func(*ddbtypes.TableDescription)
			want   contracts.StorageErrorKind
		}{
			{name: "single-region", mutate: func(*ddbtypes.TableDescription) {}},
			{name: "single-region-eventual-default", mutate: func(d *ddbtypes.TableDescription) { d.MultiRegionConsistency = ddbtypes.MultiRegionConsistencyEventual }},
			{name: "mrsc", mutate: func(d *ddbtypes.TableDescription) {
				d.GlobalTableVersion = aws.String("2019.11.21")
				d.Replicas = []ddbtypes.ReplicaDescription{{RegionName: aws.String("us-east-1")}}
				d.MultiRegionConsistency = ddbtypes.MultiRegionConsistencyStrong
			}},
			{name: "mrsc-with-witness", mutate: func(d *ddbtypes.TableDescription) {
				d.GlobalTableWitnesses = []ddbtypes.GlobalTableWitnessDescription{{RegionName: aws.String("us-west-2")}}
				d.MultiRegionConsistency = ddbtypes.MultiRegionConsistencyStrong
			}},
			{name: "missing-mode-replicas", mutate: func(d *ddbtypes.TableDescription) {
				d.Replicas = []ddbtypes.ReplicaDescription{{RegionName: aws.String("us-east-1")}}
			}, want: contracts.ErrUnsupported},
			{name: "missing-mode-version", mutate: func(d *ddbtypes.TableDescription) { d.GlobalTableVersion = aws.String("2019.11.21") }, want: contracts.ErrUnsupported},
			{name: "missing-mode-witness", mutate: func(d *ddbtypes.TableDescription) {
				d.GlobalTableWitnesses = []ddbtypes.GlobalTableWitnessDescription{{RegionName: aws.String("us-west-2")}}
			}, want: contracts.ErrUnsupported},
			{name: "mrec", mutate: func(d *ddbtypes.TableDescription) {
				d.GlobalTableVersion = aws.String("2019.11.21")
				d.MultiRegionConsistency = ddbtypes.MultiRegionConsistencyEventual
			}, want: contracts.ErrUnsupported},
			{name: "unknown-mode", mutate: func(d *ddbtypes.TableDescription) { d.MultiRegionConsistency = "FUTURE" }, want: contracts.ErrUnsupported},
			{name: "mrsc-wrong-schema", mutate: func(d *ddbtypes.TableDescription) {
				d.MultiRegionConsistency = ddbtypes.MultiRegionConsistencyStrong
				d.KeySchema = d.KeySchema[:1]
			}, want: contracts.ErrUnsupported},
			{name: "mrsc-inactive", mutate: func(d *ddbtypes.TableDescription) {
				d.MultiRegionConsistency = ddbtypes.MultiRegionConsistencyStrong
				d.TableStatus = ddbtypes.TableStatusCreating
			}, want: contracts.ErrUnavailable},
		} {
			t.Run(fmt.Sprintf("allow=%t/%s", allow, tc.name), func(t *testing.T) {
				table := compatibleTable()
				tc.mutate(table)
				p, err := NewFromConfigWithOptions(aws.Config{Region: "us-east-2", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}, AWSProviderOptions{AllowMRSC: allow})
				if err != nil {
					t.Fatal(err)
				}
				p.dynamo = &fakeDynamo{describe: func(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
					return &dynamodb.DescribeTableOutput{Table: table}, nil
				}}
				want := tc.want
				if !allow && table.MultiRegionConsistency == ddbtypes.MultiRegionConsistencyStrong {
					want = contracts.ErrUnsupported
				}
				store, err := p.OpenKeyValueStore(context.Background(), "table")
				if want == "" {
					if err != nil || store == nil {
						t.Fatalf("store=%v err=%v", store, err)
					}
				} else if !errors.Is(err, want) || store != nil {
					t.Fatalf("store=%v err=%v want=%v", store, err, want)
				}
			})
		}
	}
}

func TestMRSCConstructorDefaults(t *testing.T) {
	cfg := aws.Config{Region: "us-east-2", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
	p, err := NewFromConfig(cfg)
	if err != nil || p.allowMRSC {
		t.Fatalf("legacy constructor changed: %v", err)
	}
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/absent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/absent")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	for _, allow := range []bool{false, true} {
		p, err := New(context.Background(), AWSProviderConfig{Region: "us-east-2", AllowMRSC: allow})
		if err != nil || p.allowMRSC != allow {
			t.Fatalf("AllowMRSC=%t: %v", allow, err)
		}
	}
}

func TestMRSCWriteContentionIsDefinitiveAndNotRetried(t *testing.T) {
	for _, op := range []string{"create", "replace", "delete"} {
		t.Run(op, func(t *testing.T) {
			calls := 0
			p, err := NewFromConfigWithOptions(aws.Config{Region: "us-east-2", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 5, HTTPClient: httpClientFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}}, Body: io.NopCloser(strings.NewReader(`{"__type":"ReplicatedWriteConflictException","message":"concurrent regional write"}`)), Request: r}, nil
			})}, AWSProviderOptions{AllowMRSC: true})
			if err != nil {
				t.Fatal(err)
			}
			s := &dynamoStore{client: p.dynamo, table: "table"}
			item := kv.KeyValueItem{PartitionKey: "stream", SortKey: "revision"}
			switch op {
			case "create":
				_, err = s.Create(context.Background(), item)
			case "replace":
				_, err = s.Replace(context.Background(), item, "expected")
			case "delete":
				err = s.Delete(context.Background(), item.Key(), "expected")
			}
			var cause *ddbtypes.ReplicatedWriteConflictException
			if calls != 1 || !errors.Is(err, contracts.ErrUnavailable) || !errors.As(err, &cause) || errors.Is(err, contracts.ErrOutcomeUnknown) || errors.Is(err, contracts.ErrAlreadyExists) || errors.Is(err, contracts.ErrConflict) {
				t.Fatalf("calls=%d err=%v cause=%T", calls, err, cause)
			}
		})
	}
}

func TestMRSCSDKValidationAndConsistentReads(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(fmt.Sprintf("allow=%t", allow), func(t *testing.T) {
			calls := 0
			const arn = "arn:aws:dynamodb:us-east-2:123456789012:table/events"
			p, err := NewFromConfigWithOptions(aws.Config{Region: "us-east-2", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), HTTPClient: httpClientFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				var input map[string]any
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
					t.Fatal(err)
				}
				body := `{}`
				switch r.Header.Get("X-Amz-Target") {
				case "DynamoDB_20120810.DescribeTable":
					body = `{"Table":{"TableStatus":"ACTIVE","TableId":"test-table-id","TableArn":"` + arn + `","MultiRegionConsistency":"STRONG","GlobalTableVersion":"2019.11.21","Replicas":[{"RegionName":"us-east-1"},{"RegionName":"us-west-2"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}`
				case "DynamoDB_20120810.GetItem", "DynamoDB_20120810.Query":
					if input["ConsistentRead"] != true || input["TableName"] != arn || input["IndexName"] != nil {
						t.Fatalf("read does not use authoritative regional table: %+v", input)
					}
				default:
					t.Fatalf("unexpected request: %s", r.Header.Get("X-Amz-Target"))
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})}, AWSProviderOptions{AllowMRSC: allow})
			if err != nil {
				t.Fatal(err)
			}
			store, err := p.OpenKeyValueStore(context.Background(), "events")
			if !allow {
				if !errors.Is(err, contracts.ErrUnsupported) || calls != 1 {
					t.Fatalf("default accepted MRSC: calls=%d err=%v", calls, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Get(context.Background(), kv.KeyValueKey{PartitionKey: "stream"}); !errors.Is(err, contracts.ErrNotFound) {
				t.Fatal(err)
			}
			if _, err := store.QueryPartition(context.Background(), kv.KeyValueQuery{PartitionKey: "stream"}); err != nil {
				t.Fatal(err)
			}
			if calls != 3 {
				t.Fatalf("unexpected requests: %d", calls)
			}
		})
	}
}
