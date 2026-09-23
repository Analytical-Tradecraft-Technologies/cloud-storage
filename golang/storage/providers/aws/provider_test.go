package awsprovider

import (
	"context"
	"errors"
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
