package awsprovider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func queryTestStore(client dynamoAPI) *dynamoStore {
	return &dynamoStore{client: client, table: "test", tableARN: "arn:aws:dynamodb:ap-southeast-2:123456789012:table/test", tableID: "unique-table-id"}
}

func queryTestRecord(t *testing.T, partition, sortKey string) map[string]types.AttributeValue {
	t.Helper()
	attrs, err := dynamoKey(kv.KeyValueKey{PartitionKey: partition, SortKey: sortKey})
	if err != nil {
		t.Fatal(err)
	}
	data, err := (kv.KeyValueDocument{"bytes": kv.Bytes([]byte{1, 2}), "integer": kv.Int64(9007199254740993), "null": kv.Null()}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	attrs[dataAttribute] = &types.AttributeValueMemberB{Value: data}
	attrs[versionAttribute] = &types.AttributeValueMemberS{Value: "version"}
	attrs[modifiedAttribute] = &types.AttributeValueMemberS{Value: "2026-09-23T10:00:00+10:00"}
	return attrs
}

func TestQueryPartitionPagination(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(map[bool]string{false: "ascending", true: "descending"}[descending], func(t *testing.T) {
			keys := []string{"", "Z", "é", "😀"}
			if descending {
				keys = []string{"😀", "é", "Z", ""}
			}
			calls := 0
			f := &fakeDynamo{query: func(in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
				if aws.ToString(in.TableName) != "test" || !aws.ToBool(in.ConsistentRead) || aws.ToBool(in.ScanIndexForward) == descending || in.IndexName != nil || in.FilterExpression != nil {
					t.Fatal("query must use the authoritative table and requested direction")
				}
				if aws.ToString(in.KeyConditionExpression) != "#pk = :pk" || in.ExpressionAttributeNames["#pk"] != "pk" || in.ExpressionAttributeValues[":pk"].(*types.AttributeValueMemberS).Value != "saccount" {
					t.Fatal("wrong exact partition condition")
				}
				wantLimit := int32(1)
				if calls > 0 {
					wantLimit = 2 // PageSize may change without invalidating the cursor.
					if in.ExclusiveStartKey["sk"].(*types.AttributeValueMemberS).Value != "s"+keys[calls-1] {
						t.Fatal("lost continuation key")
					}
				} else if in.ExclusiveStartKey != nil {
					t.Fatal("unexpected initial cursor")
				}
				if aws.ToInt32(in.Limit) != wantLimit {
					t.Fatal("wrong page bound")
				}
				// A provider may return fewer than the requested number of records.
				out := &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{queryTestRecord(t, "account", keys[calls])}}
				calls++
				if calls < len(keys) {
					out.LastEvaluatedKey, _ = dynamoKey(kv.KeyValueKey{PartitionKey: "account", SortKey: keys[calls-1]})
				}
				return out, nil
			}}
			query := kv.KeyValueQuery{PartitionKey: "account", Descending: descending, PageSize: 1}
			var got []string
			for {
				// New handles accept tokens emitted before a process/store reopen.
				page, err := queryTestStore(f).QueryPartition(context.Background(), query)
				if err != nil {
					t.Fatal(err)
				}
				for _, record := range page.Records {
					got = append(got, record.Item.SortKey)
					if record.Version != "version" || record.LastModifiedAt.Location() != time.UTC || record.LastModifiedAt.Hour() != 0 || record.Item.Fields["integer"].(kv.KeyValueInt64).Value() != 9007199254740993 || record.Item.Fields["null"].Kind() != kv.FieldNull {
						t.Fatal("query lost record types or metadata")
					}
				}
				if page.NextPageToken == "" {
					break
				}
				query.PageToken, query.PageSize = page.NextPageToken, 2
			}
			if !reflect.DeepEqual(got, keys) {
				t.Fatalf("sort order: got %q want %q", got, keys)
			}
		})
	}
}

func TestQueryPrefixEmptyPagesAndDocumentOwnership(t *testing.T) {
	stored := queryTestRecord(t, "account", "run/0001")
	calls := 0
	f := &fakeDynamo{query: func(in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if aws.ToString(in.KeyConditionExpression) != "#pk = :pk AND begins_with(#sk, :prefix)" || in.ExpressionAttributeNames["#sk"] != "sk" || in.ExpressionAttributeValues[":prefix"].(*types.AttributeValueMemberS).Value != "srun/" || aws.ToInt32(in.Limit) != 100 {
			t.Fatal("prefix must use the encoded sort key and default page size")
		}
		calls++
		if calls == 1 {
			key, _ := dynamoKey(kv.KeyValueKey{PartitionKey: "account", SortKey: "run/0000"})
			return &dynamodb.QueryOutput{LastEvaluatedKey: key}, nil
		}
		return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{stored}}, nil
	}}
	store := queryTestStore(f)
	query := kv.KeyValueQuery{PartitionKey: "account", SortKeyPrefix: "run/"}
	first, err := store.QueryPartition(context.Background(), query)
	if err != nil || len(first.Records) != 0 || first.NextPageToken == "" {
		t.Fatalf("empty intermediate page: %+v %v", first, err)
	}
	query.PageToken = first.NextPageToken
	second, err := store.QueryPartition(context.Background(), query)
	if err != nil || len(second.Records) != 1 || second.NextPageToken != "" {
		t.Fatalf("terminal page: %+v %v", second, err)
	}
	second.Records[0].Item.Fields["bytes"].(kv.KeyValueBytes)[0] = 9
	again, err := store.QueryPartition(context.Background(), query)
	if err != nil || again.Records[0].Item.Fields["bytes"].(kv.KeyValueBytes)[0] != 1 {
		t.Fatal("query results alias provider memory")
	}
}

func TestQueryValidatesArgumentsAndCursorScopeBeforeIO(t *testing.T) {
	store := queryTestStore(&fakeDynamo{}) // Unexpected I/O fails the test.
	query := kv.KeyValueQuery{PartitionKey: "account", SortKeyPrefix: "run/"}
	data, _ := json.Marshal(queryCursor{Version: 1, Binding: store.queryBinding(query), SortKey: "run/0001"})
	token := base64.RawURLEncoding.EncodeToString(data)
	invalid := []kv.KeyValueQuery{
		{}, {PartitionKey: "\xff"}, {PartitionKey: strings.Repeat("p", 2048)},
		{PartitionKey: "account", SortKeyPrefix: "\xff"}, {PartitionKey: "account", SortKeyPrefix: strings.Repeat("s", 1024)},
		{PartitionKey: "account", PageSize: -1}, {PartitionKey: "account", PageSize: 1001},
		{PartitionKey: "account", PageToken: "!"}, {PartitionKey: "account", PageToken: strings.Repeat("a", maxQueryTokenBytes+1)},
		{PartitionKey: "account", PageToken: base64.RawURLEncoding.EncodeToString([]byte(`{"v":1}`))},
		{PartitionKey: "other", SortKeyPrefix: "run/", PageToken: token},
		{PartitionKey: "account", SortKeyPrefix: "different/", PageToken: token},
		{PartitionKey: "account", SortKeyPrefix: "run/", Descending: true, PageToken: token},
	}
	for _, q := range invalid {
		if _, err := store.QueryPartition(context.Background(), q); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatalf("invalid query accepted: %v", err)
		}
	}
	query.PageToken = token
	for _, change := range []func(*dynamoStore){
		func(s *dynamoStore) { s.tableARN = strings.ReplaceAll(s.tableARN, "123456789012", "987654321098") },
		func(s *dynamoStore) { s.tableARN = strings.ReplaceAll(s.tableARN, "ap-southeast-2", "us-east-1") },
		func(s *dynamoStore) { s.tableARN += "-different" },
		func(s *dynamoStore) { s.tableID = "recreated-table" },
	} {
		other := *store
		change(&other)
		if _, err := other.QueryPartition(context.Background(), query); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatal("cross-store cursor accepted")
		}
	}
	for _, cursor := range []queryCursor{
		{Version: 2, Binding: store.queryBinding(query), SortKey: "run/0001"},
		{Version: 1, Binding: store.queryBinding(query), SortKey: "outside-prefix"},
		{Version: 1, Binding: store.queryBinding(query), SortKey: "run/" + strings.Repeat("x", 1024)},
	} {
		data, _ := json.Marshal(cursor)
		query.PageToken = base64.RawURLEncoding.EncodeToString(data)
		if _, err := store.QueryPartition(context.Background(), query); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatal("invalid cursor accepted")
		}
	}
}

func TestQueryMaximumEscapedKeyCursorRoundTrip(t *testing.T) {
	partition, sortKey := strings.Repeat("\x00", 2047), strings.Repeat("\x00", 1023)
	calls := 0
	f := &fakeDynamo{query: func(in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if aws.ToInt32(in.Limit) != 1000 {
			t.Fatal("maximum page size rejected")
		}
		calls++
		if calls == 1 {
			key, _ := dynamoKey(kv.KeyValueKey{PartitionKey: partition, SortKey: sortKey})
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{queryTestRecord(t, partition, sortKey)}, LastEvaluatedKey: key}, nil
		}
		return &dynamodb.QueryOutput{}, nil
	}}
	store := queryTestStore(f)
	query := kv.KeyValueQuery{PartitionKey: partition, PageSize: 1000}
	first, err := store.QueryPartition(context.Background(), query)
	if err != nil || len(first.NextPageToken) > maxQueryTokenBytes {
		t.Fatalf("valid key cannot paginate: %v", err)
	}
	query.PageToken = first.NextPageToken
	last, err := store.QueryPartition(context.Background(), query)
	if err != nil || len(last.Records) != 0 || last.NextPageToken != "" {
		t.Fatalf("empty final page after valid token: %+v %v", last, err)
	}
}

func TestQueryRejectsCorruptBackendResponses(t *testing.T) {
	for name, output := range map[string]func() *dynamodb.QueryOutput{
		"nil": func() *dynamodb.QueryOutput { return nil },
		"bad document": func() *dynamodb.QueryOutput {
			row := queryTestRecord(t, "account", "run/1")
			row[dataAttribute] = &types.AttributeValueMemberB{Value: []byte("invalid")}
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{row}}
		},
		"wrong partition": func() *dynamodb.QueryOutput {
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{queryTestRecord(t, "other", "run/1")}}
		},
		"wrong prefix": func() *dynamodb.QueryOutput {
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{queryTestRecord(t, "account", "other/1")}}
		},
		"out of order": func() *dynamodb.QueryOutput {
			return &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{queryTestRecord(t, "account", "run/2"), queryTestRecord(t, "account", "run/1")}}
		},
		"bad continuation": func() *dynamodb.QueryOutput {
			key, _ := dynamoKey(kv.KeyValueKey{PartitionKey: "other", SortKey: "run/1"})
			return &dynamodb.QueryOutput{LastEvaluatedKey: key}
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := queryTestStore(&fakeDynamo{query: func(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error) { return output(), nil }})
			page, err := store.QueryPartition(context.Background(), kv.KeyValueQuery{PartitionKey: "account", SortKeyPrefix: "run/"})
			if !errors.Is(err, contracts.ErrUnknown) || errors.Is(err, contracts.ErrInvalidArgument) || len(page.Records) != 0 || page.NextPageToken != "" {
				t.Fatalf("corrupt response leaked a partial result: %+v %v", page, err)
			}
		})
	}
}

func TestQueryErrorsPreserveCauseWithoutUnknownMutation(t *testing.T) {
	cause := &types.ProvisionedThroughputExceededException{}
	calls := 0
	store := queryTestStore(&fakeDynamo{query: func(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		calls++
		return nil, cause
	}})
	_, err := store.QueryPartition(context.Background(), kv.KeyValueQuery{PartitionKey: "account"})
	if !errors.Is(err, contracts.ErrThrottled) || !errors.Is(err, cause) || errors.Is(err, contracts.ErrOutcomeUnknown) || calls != 1 {
		t.Fatalf("query classification/retry: %v, calls %d", err, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = store.QueryPartition(ctx, kv.KeyValueQuery{PartitionKey: "account"})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, contracts.ErrCanceled) || errors.Is(err, contracts.ErrOutcomeUnknown) || calls != 1 {
		t.Fatalf("query cancellation: %v", err)
	}
}
