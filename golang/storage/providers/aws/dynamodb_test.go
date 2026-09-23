package awsprovider

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type fakeDynamo struct {
	dynamoAPI
	query    func(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
	get      func(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	put      func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	delete   func(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	describe func(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)
	list     func(*dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error)
}

func (f *fakeDynamo) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return f.query(in)
}
func (f *fakeDynamo) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return f.get(in)
}
func (f *fakeDynamo) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return f.put(in)
}
func (f *fakeDynamo) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return f.delete(in)
}
func (f *fakeDynamo) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return f.describe(in)
}
func (f *fakeDynamo) ListTables(_ context.Context, in *dynamodb.ListTablesInput, _ ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	return f.list(in)
}

func TestDynamoRecordRoundTripAndIsolation(t *testing.T) {
	ctx := context.Background()
	var rows = map[string]map[string]types.AttributeValue{}
	f := &fakeDynamo{
		put: func(in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			if aws.ToString(in.ConditionExpression) != "attribute_not_exists(#pk)" || in.ExpressionAttributeNames["#pk"] != "pk" {
				t.Fatal("create is not conditional")
			}
			if in.Item["sk"].(*types.AttributeValueMemberS).Value != "s" {
				t.Fatal("empty sort key not encoded")
			}
			if in.Item["pk"].(*types.AttributeValueMemberS).Value != "saccount" {
				t.Fatal("partition not encoded")
			}
			rows[*in.TableName] = in.Item
			return &dynamodb.PutItemOutput{}, nil
		},
		get: func(in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			if !aws.ToBool(in.ConsistentRead) {
				t.Fatal("eventually consistent read")
			}
			return &dynamodb.GetItemOutput{Item: rows[*in.TableName]}, nil
		},
	}
	a := &dynamoStore{client: f, table: "table-a"}
	b := &dynamoStore{client: f, table: "table-b"}
	item := kv.KeyValueItem{PartitionKey: "account", Fields: kv.KeyValueDocument{
		"integer": kv.Int64(math.MaxInt64), "float": kv.Float64(42), "bytes": kv.Bytes([]byte{1, 2}),
		"nested": kv.List(kv.Document(kv.KeyValueDocument{"null": kv.Null()})),
	}}
	before := time.Now().UTC()
	version, err := a.Create(ctx, item)
	if err != nil {
		t.Fatal(err)
	}
	item.Fields["integer"] = kv.Int64(0)
	record, err := a.Get(ctx, item.Key())
	if err != nil {
		t.Fatal(err)
	}
	if record.Version != version || len(version) != 64 || record.LastModifiedAt.Before(before) || record.LastModifiedAt.After(time.Now()) {
		t.Fatal("invalid metadata")
	}
	if record.Item.Fields["integer"].(kv.KeyValueInt64).Value() != math.MaxInt64 || record.Item.Fields["float"].Kind() != kv.FieldFloat64 {
		t.Fatal("types or ownership lost")
	}
	record.Item.Fields["bytes"].(kv.KeyValueBytes)[0] = 9
	again, err := a.Get(ctx, item.Key())
	if err != nil {
		t.Fatal(err)
	}
	if again.Item.Fields["bytes"].(kv.KeyValueBytes)[0] != 1 {
		t.Fatal("read buffers alias persisted record")
	}
	if _, err := b.Get(ctx, item.Key()); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatal("tables are not isolated")
	}
	version2, err := a.Create(ctx, item)
	if err != nil {
		t.Fatal(err)
	}
	if version2 == version {
		t.Fatal("version reused")
	}
}

func TestDynamoConditionalMutations(t *testing.T) {
	ctx := context.Background()
	condition := &types.ConditionalCheckFailedException{}
	f := &fakeDynamo{put: func(in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		if in.ConditionExpression == nil {
			t.Fatal("unconditional write")
		}
		if *in.ConditionExpression == "#version = :expected" {
			if in.ExpressionAttributeNames["#version"] != versionAttribute || in.ExpressionAttributeValues[":expected"].(*types.AttributeValueMemberS).Value != "expected" {
				t.Fatal("missing version condition")
			}
		}
		return nil, condition
	}, delete: func(in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		if aws.ToString(in.ConditionExpression) != "#version = :expected" || in.ExpressionAttributeValues[":expected"].(*types.AttributeValueMemberS).Value != "expected" {
			t.Fatal("unconditional deletion")
		}
		return nil, condition
	}}
	s := &dynamoStore{client: f, table: "test"}
	item := kv.KeyValueItem{PartitionKey: "p", SortKey: "s"}
	_, err := s.Create(ctx, item)
	if !errors.Is(err, contracts.ErrAlreadyExists) || !errors.Is(err, condition) || errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatalf("bad create failure: %v", err)
	}
	_, err = s.Replace(ctx, item, "expected")
	if !errors.Is(err, contracts.ErrConflict) || !errors.Is(err, condition) {
		t.Fatalf("bad replace failure: %v", err)
	}
	if err = s.Delete(ctx, item.Key(), "expected"); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("bad delete failure: %v", err)
	}
}

func TestDynamoRejectsInvalidInputBeforeIO(t *testing.T) {
	s := &dynamoStore{client: &fakeDynamo{}, table: "test"}
	for _, item := range []kv.KeyValueItem{
		{}, {PartitionKey: strings.Repeat("p", 2048)}, {PartitionKey: "p", SortKey: strings.Repeat("s", 1024)},
		{PartitionKey: "p", Fields: kv.KeyValueDocument{"bad": kv.Float64(math.NaN())}},
		{PartitionKey: "p", Fields: kv.KeyValueDocument{"large": kv.Bytes(make([]byte, 400*1024))}},
	} {
		if _, err := s.Create(context.Background(), item); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatalf("bad validation: %v", err)
		}
	}
	item := kv.KeyValueItem{PartitionKey: "p"}
	if _, err := s.Replace(context.Background(), item, ""); !errors.Is(err, contracts.ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := s.Delete(context.Background(), item.Key(), ""); !errors.Is(err, contracts.ErrInvalidArgument) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Create(ctx, item); !errors.Is(err, context.Canceled) || errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatal(err)
	}
}

func TestDynamoConcurrentCASOnlyOneWinner(t *testing.T) {
	var mu sync.Mutex
	current := "original"
	f := &fakeDynamo{put: func(in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		mu.Lock()
		defer mu.Unlock()
		expected := in.ExpressionAttributeValues[":expected"].(*types.AttributeValueMemberS).Value
		if expected != current {
			return nil, &types.ConditionalCheckFailedException{}
		}
		current = in.Item[versionAttribute].(*types.AttributeValueMemberS).Value
		return &dynamodb.PutItemOutput{}, nil
	}}
	s := &dynamoStore{client: f, table: "test"}
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := s.Replace(context.Background(), kv.KeyValueItem{PartitionKey: "p"}, "original")
			results <- err
		}()
	}
	wins, conflicts := 0, 0
	for range 2 {
		err := <-results
		if err == nil {
			wins++
		} else if errors.Is(err, contracts.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
}

func TestDynamoCorruptStoredDataIsNotCallerError(t *testing.T) {
	f := &fakeDynamo{get: func(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
			dataAttribute: &types.AttributeValueMemberB{Value: []byte("bad")}, versionAttribute: &types.AttributeValueMemberS{Value: "v"}, modifiedAttribute: &types.AttributeValueMemberS{Value: time.Now().Format(time.RFC3339Nano)},
		}}, nil
	}}
	_, err := (&dynamoStore{client: f, table: "test"}).Get(context.Background(), kv.KeyValueKey{PartitionKey: "p"})
	if !errors.Is(err, contracts.ErrUnknown) || errors.Is(err, contracts.ErrInvalidArgument) {
		t.Fatal(err)
	}
}
