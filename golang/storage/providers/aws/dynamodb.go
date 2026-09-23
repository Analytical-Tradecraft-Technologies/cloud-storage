package awsprovider

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
	"unicode/utf8"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	dataAttribute      = "_cs_document"
	versionAttribute   = "_cs_version"
	modifiedAttribute  = "_cs_modified"
	maxDynamoItemBytes = 400 * 1024
)

type dynamoStore struct {
	client   dynamoAPI
	table    string
	tableARN string
	tableID  string
}

var _ kv.KeyValueStore = (*dynamoStore)(nil)

func dynamoKey(key kv.KeyValueKey) (map[string]types.AttributeValue, error) {
	// A constant prefix permits the logical empty sort key and is collision-free.
	if key.PartitionKey == "" || !utf8.ValidString(key.PartitionKey) || !utf8.ValidString(key.SortKey) || len(key.PartitionKey) > 2047 || len(key.SortKey) > 1023 {
		return nil, failure("kv.key", contracts.ErrInvalidArgument, nil)
	}
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "s" + key.PartitionKey},
		"sk": &types.AttributeValueMemberS{Value: "s" + key.SortKey},
	}, nil
}

func (s *dynamoStore) Get(ctx context.Context, key kv.KeyValueKey) (kv.KeyValueRecord, error) {
	const op = "kv.get"
	attrs, err := dynamoKey(key)
	if err != nil {
		return kv.KeyValueRecord{}, err
	}
	if err := ctx.Err(); err != nil {
		return kv.KeyValueRecord{}, wrapError(ctx, op, err, false)
	}
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(s.table), Key: attrs, ConsistentRead: aws.Bool(true)})
	if err != nil {
		return kv.KeyValueRecord{}, wrapError(ctx, op, err, false)
	}
	if out == nil {
		return kv.KeyValueRecord{}, failure(op, contracts.ErrUnknown, nil)
	}
	if len(out.Item) == 0 {
		return kv.KeyValueRecord{}, failure(op, contracts.ErrNotFound, nil)
	}
	return decodeDynamoRecord(op, key, out.Item)
}

func decodeDynamoRecord(op string, key kv.KeyValueKey, attrs map[string]types.AttributeValue) (kv.KeyValueRecord, error) {
	data, dataOK := attrs[dataAttribute].(*types.AttributeValueMemberB)
	version, versionOK := attrs[versionAttribute].(*types.AttributeValueMemberS)
	modified, modifiedOK := attrs[modifiedAttribute].(*types.AttributeValueMemberS)
	if !dataOK || data == nil || !versionOK || version == nil || version.Value == "" || !modifiedOK || modified == nil {
		return kv.KeyValueRecord{}, failure(op, contracts.ErrUnknown, nil)
	}
	var doc kv.KeyValueDocument
	if err := doc.UnmarshalBinary(data.Value); err != nil {
		// Invalid stored data is not a caller argument error.
		return kv.KeyValueRecord{}, failure(op, contracts.ErrUnknown, errors.New("invalid stored document encoding"))
	}
	timestamp, err := time.Parse(time.RFC3339Nano, modified.Value)
	if err != nil || timestamp.IsZero() {
		return kv.KeyValueRecord{}, failure(op, contracts.ErrUnknown, errors.New("invalid stored modification time"))
	}
	return kv.KeyValueRecord{Item: kv.KeyValueItem{PartitionKey: key.PartitionKey, SortKey: key.SortKey, Fields: doc}, Version: kv.KeyValueVersion(version.Value), LastModifiedAt: timestamp.UTC()}, nil
}

func (s *dynamoStore) Create(ctx context.Context, item kv.KeyValueItem) (kv.KeyValueVersion, error) {
	return s.put(ctx, item, "", true)
}
func (s *dynamoStore) Replace(ctx context.Context, item kv.KeyValueItem, expected kv.KeyValueVersion) (kv.KeyValueVersion, error) {
	if expected == "" {
		return "", failure("kv.replace", contracts.ErrInvalidArgument, nil)
	}
	return s.put(ctx, item, expected, false)
}

func (s *dynamoStore) put(ctx context.Context, item kv.KeyValueItem, expected kv.KeyValueVersion, create bool) (kv.KeyValueVersion, error) {
	op := "kv.replace"
	if create {
		op = "kv.create"
	}
	attrs, err := dynamoKey(item.Key())
	if err != nil {
		return "", err
	}
	data, err := item.Fields.MarshalBinary()
	if err != nil {
		return "", failure(op, contracts.ErrInvalidArgument, err)
	}
	if err := ctx.Err(); err != nil {
		return "", wrapError(ctx, op, err, false)
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", failure(op, contracts.ErrUnknown, err)
	}
	version := hex.EncodeToString(random[:])
	modified := time.Now().UTC().Format(time.RFC3339Nano)
	size := len("pk") + 1 + len(item.PartitionKey) + len("sk") + 1 + len(item.SortKey) + len(dataAttribute) + len(data) + len(versionAttribute) + len(version) + len(modifiedAttribute) + len(modified)
	if size > maxDynamoItemBytes {
		return "", failure(op, contracts.ErrInvalidArgument, nil)
	}
	attrs[dataAttribute] = &types.AttributeValueMemberB{Value: data}
	attrs[versionAttribute] = &types.AttributeValueMemberS{Value: version}
	attrs[modifiedAttribute] = &types.AttributeValueMemberS{Value: modified}
	input := &dynamodb.PutItemInput{TableName: aws.String(s.table), Item: attrs}
	if create {
		input.ConditionExpression = aws.String("attribute_not_exists(#pk)")
		input.ExpressionAttributeNames = map[string]string{"#pk": "pk"}
	} else {
		input.ConditionExpression = aws.String("#version = :expected")
		input.ExpressionAttributeNames = map[string]string{"#version": versionAttribute}
		input.ExpressionAttributeValues = map[string]types.AttributeValue{":expected": &types.AttributeValueMemberS{Value: string(expected)}}
	}
	_, err = s.client.PutItem(ctx, input)
	if err != nil {
		var condition *types.ConditionalCheckFailedException
		if errors.As(err, &condition) {
			kind := contracts.ErrConflict
			if create {
				kind = contracts.ErrAlreadyExists
			}
			return "", failure(op, kind, err)
		}
		return "", wrapError(ctx, op, err, true)
	}
	return kv.KeyValueVersion(version), nil
}

func (s *dynamoStore) Delete(ctx context.Context, key kv.KeyValueKey, expected kv.KeyValueVersion) error {
	const op = "kv.delete"
	attrs, err := dynamoKey(key)
	if err != nil {
		return err
	}
	if expected == "" {
		return failure(op, contracts.ErrInvalidArgument, nil)
	}
	if err := ctx.Err(); err != nil {
		return wrapError(ctx, op, err, false)
	}
	_, err = s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.table), Key: attrs,
		ConditionExpression:       aws.String("#version = :expected"),
		ExpressionAttributeNames:  map[string]string{"#version": versionAttribute},
		ExpressionAttributeValues: map[string]types.AttributeValue{":expected": &types.AttributeValueMemberS{Value: string(expected)}},
	})
	if err != nil {
		return wrapError(ctx, op, err, true)
	}
	return nil
}
