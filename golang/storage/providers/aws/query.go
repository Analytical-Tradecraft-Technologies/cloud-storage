package awsprovider

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const queryOperation = "kv.query_partition"
const maxQueryTokenBytes = 16 * 1024

type queryCursor struct {
	Version int    `json:"v"`
	Binding string `json:"q"`
	SortKey string `json:"s"`
}

// Binding prevents accidental cross-query reuse; it is not an authentication tag.
func (s *dynamoStore) queryBinding(q kv.KeyValueQuery) string {
	data, _ := json.Marshal(struct {
		ARN, ID, Partition, Prefix string
		PageSize                   int
	}{s.tableARN, s.tableID, q.PartitionKey, q.SortKeyPrefix, queryPageSize(q)})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (s *dynamoStore) queryStart(q kv.KeyValueQuery) (map[string]types.AttributeValue, error) {
	if q.PageToken == "" {
		return nil, nil
	}
	if len(q.PageToken) > maxQueryTokenBytes {
		return nil, failure(queryOperation, contracts.ErrInvalidArgument, nil)
	}
	data, err := base64.RawURLEncoding.DecodeString(q.PageToken)
	if err != nil {
		return nil, failure(queryOperation, contracts.ErrInvalidArgument, nil)
	}
	var cursor queryCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil, failure(queryOperation, contracts.ErrInvalidArgument, nil)
	}
	// Accept only the canonical encoding we emit, including known fields once.
	canonical, _ := json.Marshal(cursor)
	if string(data) != string(canonical) || cursor.Version != 1 || cursor.Binding != s.queryBinding(q) || !strings.HasPrefix(cursor.SortKey, q.SortKeyPrefix) {
		return nil, failure(queryOperation, contracts.ErrInvalidArgument, nil)
	}
	key, err := dynamoKey(kv.KeyValueKey{PartitionKey: q.PartitionKey, SortKey: cursor.SortKey})
	if err != nil {
		return nil, failure(queryOperation, contracts.ErrInvalidArgument, nil)
	}
	return key, nil
}

func queryKey(attrs map[string]types.AttributeValue, q kv.KeyValueQuery) (kv.KeyValueKey, error) {
	pk, pkOK := attrs["pk"].(*types.AttributeValueMemberS)
	sk, skOK := attrs["sk"].(*types.AttributeValueMemberS)
	if !pkOK || pk == nil || !skOK || sk == nil || pk.Value != "s"+q.PartitionKey || !strings.HasPrefix(sk.Value, "s"+q.SortKeyPrefix) {
		return kv.KeyValueKey{}, failure(queryOperation, contracts.ErrUnknown, nil)
	}
	key := kv.KeyValueKey{PartitionKey: q.PartitionKey, SortKey: sk.Value[1:]}
	if _, err := dynamoKey(key); err != nil {
		return kv.KeyValueKey{}, failure(queryOperation, contracts.ErrUnknown, nil)
	}
	return key, nil
}

func queryPageSize(q kv.KeyValueQuery) int {
	if q.PageSize == 0 {
		return 100
	}
	return q.PageSize
}

func (s *dynamoStore) QueryPartition(ctx context.Context, q kv.KeyValueQuery) (kv.KeyValueQueryPage, error) {
	if _, err := dynamoKey(kv.KeyValueKey{PartitionKey: q.PartitionKey, SortKey: q.SortKeyPrefix}); err != nil || q.PageSize < 0 || q.PageSize > 1000 {
		return kv.KeyValueQueryPage{}, failure(queryOperation, contracts.ErrInvalidArgument, nil)
	}
	limit := queryPageSize(q)
	start, err := s.queryStart(q)
	if err != nil {
		return kv.KeyValueQueryPage{}, err
	}
	if err := ctx.Err(); err != nil {
		return kv.KeyValueQueryPage{}, wrapError(ctx, queryOperation, err, false)
	}
	input := &dynamodb.QueryInput{
		TableName: aws.String(s.table), ConsistentRead: aws.Bool(true),
		ScanIndexForward: aws.Bool(true), Limit: aws.Int32(int32(limit)),
		KeyConditionExpression:    aws.String("#pk = :pk"),
		ExpressionAttributeNames:  map[string]string{"#pk": "pk"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "s" + q.PartitionKey}},
		ExclusiveStartKey:         start,
	}
	if q.SortKeyPrefix != "" {
		input.KeyConditionExpression = aws.String("#pk = :pk AND begins_with(#sk, :prefix)")
		input.ExpressionAttributeNames["#sk"] = "sk"
		input.ExpressionAttributeValues[":prefix"] = &types.AttributeValueMemberS{Value: "s" + q.SortKeyPrefix}
	}
	out, err := s.client.Query(ctx, input)
	if err != nil {
		return kv.KeyValueQueryPage{}, wrapError(ctx, queryOperation, err, false)
	}
	if out == nil || len(out.Items) > limit {
		return kv.KeyValueQueryPage{}, failure(queryOperation, contracts.ErrUnknown, nil)
	}
	page := kv.KeyValueQueryPage{Records: make([]kv.KeyValueRecord, 0, len(out.Items))}
	var previous string
	hasPrevious := start != nil
	if hasPrevious {
		previous = start["sk"].(*types.AttributeValueMemberS).Value[1:]
	}
	for _, attrs := range out.Items {
		key, err := queryKey(attrs, q)
		if err != nil {
			return kv.KeyValueQueryPage{}, err
		}
		if hasPrevious && key.SortKey <= previous {
			return kv.KeyValueQueryPage{}, failure(queryOperation, contracts.ErrUnknown, nil)
		}
		record, err := decodeDynamoRecord(queryOperation, key, attrs)
		if err != nil {
			return kv.KeyValueQueryPage{}, err
		}
		page.Records = append(page.Records, record)
		previous, hasPrevious = key.SortKey, true
	}
	if len(out.LastEvaluatedKey) != 0 {
		last, err := queryKey(out.LastEvaluatedKey, q)
		if err != nil {
			return kv.KeyValueQueryPage{}, err
		}
		if len(out.LastEvaluatedKey) != 2 || (hasPrevious && last.SortKey < previous) || (len(out.Items) == 0 && hasPrevious && last.SortKey == previous) {
			return kv.KeyValueQueryPage{}, failure(queryOperation, contracts.ErrUnknown, nil)
		}
		data, _ := json.Marshal(queryCursor{Version: 1, Binding: s.queryBinding(q), SortKey: last.SortKey})
		page.NextPageToken = base64.RawURLEncoding.EncodeToString(data)
	}
	return page, nil
}
