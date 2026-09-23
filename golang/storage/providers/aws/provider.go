// Package awsprovider implements storage contracts using DynamoDB and S3.
// Resources must already exist. One provider represents one AWS region and
// credential configuration; authentication uses AWS IAM request signing.
package awsprovider

import (
	"context"
	"regexp"
	"strings"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// AWSProviderConfig selects a region and optional shared AWS profile. Empty
// values use the SDK's environment/shared configuration and IAM credential chain.
// TempDirectory holds private staging files for exact-length blob validation.
// Empty TempDirectory uses the operating system's temporary directory.
type AWSProviderConfig struct {
	Region        string
	Profile       string
	TempDirectory string
}

// AWSStorageProvider shares SDK clients across lightweight table/bucket handles.
// Create a separate provider for each region or credential configuration.
type AWSStorageProvider struct {
	dynamo        dynamoAPI
	s3            s3API
	region        string
	tempDirectory string
}

var _ provider.StorageProvider = (*AWSStorageProvider)(nil)

// New loads the default SDK credential chain, including renewable role credentials
// and shared profiles. It does not list or provision resources.
func New(ctx context.Context, cfg AWSProviderConfig) (*AWSStorageProvider, error) {
	opts := []func(*config.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}
	if cfg.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(cfg.Profile))
	}
	sdk, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, wrapError(ctx, "provider.configure", err, false)
	}
	p, err := NewFromConfig(sdk)
	if err != nil {
		return nil, err
	}
	p.tempDirectory = cfg.TempDirectory
	return p, nil
}

// NewFromConfig accepts an already configured AWS SDK, including an assumed-role
// credentials provider or custom HTTP transport. A region and authenticated
// credentials provider are required. SDK request logging is disabled. Retryers
// are forced to single-attempt to avoid disguising uncertain mutation outcomes.
// Use New when a custom staging directory is needed.
func NewFromConfig(cfg aws.Config) (*AWSStorageProvider, error) {
	if cfg.Region == "" || cfg.Credentials == nil {
		return nil, failure("provider.configure", contracts.ErrInvalidArgument, nil)
	}
	switch cfg.Credentials.(type) {
	case aws.AnonymousCredentials, *aws.AnonymousCredentials:
		return nil, failure("provider.configure", contracts.ErrUnauthenticated, nil)
	}
	cfg.ClientLogMode = 0
	cfg.Retryer = func() aws.Retryer { return aws.NopRetryer{} }
	return &AWSStorageProvider{dynamo: dynamodb.NewFromConfig(cfg), s3: s3.NewFromConfig(cfg), region: cfg.Region}, nil
}

var tableName = regexp.MustCompile(`^[a-zA-Z0-9_.-]{3,255}$`)
var bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// OpenKeyValueStore validates a single-region table with string pk/sk keys.
// This adapter exclusively owns the record format in that table; arbitrary
// existing DynamoDB records are not automatically imported.
func (p *AWSStorageProvider) OpenKeyValueStore(ctx context.Context, name string) (kv.KeyValueStore, error) {
	const op = "provider.open_kv"
	if !tableName.MatchString(name) {
		return nil, failure(op, contracts.ErrInvalidArgument, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, wrapError(ctx, op, err, false)
	}
	out, err := p.dynamo.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	if err != nil {
		return nil, wrapError(ctx, op, err, false)
	}
	if out == nil || out.Table == nil {
		return nil, failure(op, contracts.ErrUnknown, nil)
	}
	table := out.Table
	if len(table.Replicas) != 0 || aws.ToString(table.GlobalTableVersion) != "" {
		return nil, failure(op, contracts.ErrUnsupported, nil)
	}
	if table.TableStatus != ddbtypes.TableStatusActive {
		return nil, failure(op, contracts.ErrUnavailable, nil)
	}
	schema := map[string]ddbtypes.KeyType{}
	for _, key := range table.KeySchema {
		schema[aws.ToString(key.AttributeName)] = key.KeyType
	}
	attributes := map[string]ddbtypes.ScalarAttributeType{}
	for _, a := range table.AttributeDefinitions {
		attributes[aws.ToString(a.AttributeName)] = a.AttributeType
	}
	if len(schema) != 2 || schema["pk"] != ddbtypes.KeyTypeHash || schema["sk"] != ddbtypes.KeyTypeRange || attributes["pk"] != ddbtypes.ScalarAttributeTypeS || attributes["sk"] != ddbtypes.ScalarAttributeTypeS {
		return nil, failure(op, contracts.ErrUnsupported, nil)
	}
	if aws.ToString(table.TableArn) == "" || aws.ToString(table.TableId) == "" {
		return nil, failure(op, contracts.ErrUnknown, nil)
	}
	return &dynamoStore{client: p.dynamo, table: *table.TableArn, tableARN: *table.TableArn, tableID: *table.TableId}, nil
}

// OpenBlobStore checks access and region for an existing general-purpose bucket.
// Directory buckets and access point ARNs are not supported. HeadBucket needs
// s3:ListBucket but opening does not require account-wide listing permission.
func (p *AWSStorageProvider) OpenBlobStore(ctx context.Context, name string) (blob.BlobStore, error) {
	const op = "provider.open_blob"
	if !bucketName.MatchString(name) {
		return nil, failure(op, contracts.ErrInvalidArgument, nil)
	}
	if strings.HasSuffix(name, "--x-s3") {
		return nil, failure(op, contracts.ErrUnsupported, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, wrapError(ctx, op, err, false)
	}
	out, err := p.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
	if err != nil {
		return nil, wrapError(ctx, op, err, false)
	}
	if out == nil || out.BucketRegion == nil {
		return nil, failure(op, contracts.ErrUnknown, nil)
	}
	if *out.BucketRegion != p.region {
		return nil, failure(op, contracts.ErrUnsupported, nil)
	}
	return &s3Store{client: p.s3, bucket: name, tempDirectory: p.tempDirectory}, nil
}

func pageSize(options provider.StoreListOptions, maximum int) (int32, error) {
	if options.PageSize < 0 || options.PageSize > maximum {
		return 0, failure("provider.list", contracts.ErrInvalidArgument, nil)
	}
	if options.PageSize == 0 {
		return 100, nil
	}
	return int32(options.PageSize), nil
}

// ListKeyValueStores lists tables in this credential scope and region. Tables
// can be listed even if their schema is incompatible or IAM denies data access.
func (p *AWSStorageProvider) ListKeyValueStores(ctx context.Context, options provider.StoreListOptions) (provider.StoreListPage, error) {
	const op = "provider.list_kv"
	size, err := pageSize(options, 100)
	if err != nil {
		return provider.StoreListPage{}, err
	}
	input := &dynamodb.ListTablesInput{Limit: aws.Int32(size)}
	if options.PageToken != "" {
		input.ExclusiveStartTableName = aws.String(options.PageToken)
	}
	out, err := p.dynamo.ListTables(ctx, input)
	if err != nil {
		return provider.StoreListPage{}, wrapError(ctx, op, err, false)
	}
	if out == nil {
		return provider.StoreListPage{}, failure(op, contracts.ErrUnknown, nil)
	}
	return provider.StoreListPage{Names: append([]string(nil), out.TableNames...), NextPageToken: aws.ToString(out.LastEvaluatedTableName)}, nil
}

// ListBlobStores lists account-owned general-purpose buckets in this region.
// Buckets shared by other accounts can be opened by name but are not listed.
func (p *AWSStorageProvider) ListBlobStores(ctx context.Context, options provider.StoreListOptions) (provider.StoreListPage, error) {
	const op = "provider.list_blob"
	size, err := pageSize(options, 10000)
	if err != nil {
		return provider.StoreListPage{}, err
	}
	input := &s3.ListBucketsInput{MaxBuckets: aws.Int32(size), BucketRegion: aws.String(p.region)}
	if options.PageToken != "" {
		input.ContinuationToken = aws.String(options.PageToken)
	}
	out, err := p.s3.ListBuckets(ctx, input)
	if err != nil {
		return provider.StoreListPage{}, wrapError(ctx, op, err, false)
	}
	if out == nil {
		return provider.StoreListPage{}, failure(op, contracts.ErrUnknown, nil)
	}
	page := provider.StoreListPage{NextPageToken: aws.ToString(out.ContinuationToken)}
	for _, b := range out.Buckets {
		if b.Name != nil {
			page.Names = append(page.Names, *b.Name)
		}
	}
	return page, nil
}
