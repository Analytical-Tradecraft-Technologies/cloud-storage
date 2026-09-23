package eventsourcing

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

const eventPrefix = "event/"
const eventFormatVersion = 1

type eventPayload struct {
	FormatVersion      uint64            `json:"format_version"`
	ApplicationVersion uint64            `json:"application_version"`
	AppendToken        string            `json:"append_token"`
	Changes            []json.RawMessage `json:"changes"`
}

func eventKey(streamID string, revision EventStreamRevision) kv.KeyValueKey {
	return kv.KeyValueKey{PartitionKey: streamID, SortKey: fmt.Sprintf("%s%020d", eventPrefix, revision)}
}

func keyRevision(key kv.KeyValueKey, streamID string) (EventStreamRevision, error) {
	if key.PartitionKey != streamID || !strings.HasPrefix(key.SortKey, eventPrefix) || len(key.SortKey) != len(eventPrefix)+20 {
		return 0, corruptHistory("event.decode", nil)
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(key.SortKey, eventPrefix), 10, 64)
	if err != nil || n == 0 || eventKey(streamID, EventStreamRevision(n)).SortKey != key.SortKey {
		return 0, corruptHistory("event.decode", err)
	}
	return EventStreamRevision(n), nil
}

func compactJSON(data json.RawMessage) (json.RawMessage, error) {
	if !utf8.Valid(data) {
		return nil, errors.New("invalid UTF-8 JSON")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, data); err != nil {
		return nil, err
	}
	return json.RawMessage(compact.Bytes()), nil
}

func encodeAppend(streamID string, revision EventStreamRevision, request EventAppend) (kv.KeyValueItem, error) {
	const op = "event.append"
	if request.AppendToken == "" || !utf8.ValidString(request.AppendToken) || len(request.Changes) == 0 {
		return kv.KeyValueItem{}, eventError(op, contracts.ErrInvalidArgument, nil)
	}
	changes := make([]json.RawMessage, len(request.Changes))
	for i, change := range request.Changes {
		encoded, err := compactJSON(change)
		if err != nil {
			return kv.KeyValueItem{}, eventError(op, contracts.ErrInvalidArgument, err)
		}
		changes[i] = encoded
	}
	payload, err := json.Marshal(eventPayload{eventFormatVersion, request.ApplicationVersion, request.AppendToken, changes})
	if err != nil {
		return kv.KeyValueItem{}, eventError(op, contracts.ErrInvalidArgument, err)
	}
	key := eventKey(streamID, revision)
	item := kv.KeyValueItem{PartitionKey: key.PartitionKey, SortKey: key.SortKey, Fields: kv.KeyValueDocument{
		"recorded_at": kv.Timestamp(time.Now().UTC()),
		"event":       kv.Bytes(payload),
	}}
	if len(request.Metadata) != 0 {
		metadata, err := compactJSON(request.Metadata)
		if err != nil {
			return kv.KeyValueItem{}, eventError(op, contracts.ErrInvalidArgument, err)
		}
		item.Fields["metadata"] = kv.Bytes(metadata)
	}
	return item, nil
}

// Decode envelope fields explicitly to reject duplicate/missing fields. The
// application JSON inside metadata/changes stays opaque and is never converted
// through interface{} or float64.
func decodePayload(data []byte) (eventPayload, error) {
	const op = "event.decode"
	var payload eventPayload
	if !utf8.Valid(data) {
		return payload, corruptHistory(op, nil)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return payload, corruptHistory(op, err)
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		name, err := decoder.Token()
		if err != nil {
			return payload, corruptHistory(op, err)
		}
		key, ok := name.(string)
		if !ok {
			return payload, corruptHistory(op, nil)
		}
		if _, exists := fields[key]; exists {
			return payload, corruptHistory(op, nil)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return payload, corruptHistory(op, err)
		}
		fields[key] = value
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return payload, corruptHistory(op, err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return payload, corruptHistory(op, err)
	}
	if len(fields["format_version"]) == 0 || bytes.Equal(fields["format_version"], []byte("null")) || json.Unmarshal(fields["format_version"], &payload.FormatVersion) != nil {
		return payload, corruptHistory(op, nil)
	}
	if payload.FormatVersion != eventFormatVersion {
		return payload, eventError(op, contracts.ErrUnsupported, nil)
	}
	if len(fields) != 4 || len(fields["application_version"]) == 0 || bytes.Equal(fields["application_version"], []byte("null")) || json.Unmarshal(fields["application_version"], &payload.ApplicationVersion) != nil || json.Unmarshal(fields["append_token"], &payload.AppendToken) != nil || payload.AppendToken == "" || json.Unmarshal(fields["changes"], &payload.Changes) != nil || len(payload.Changes) == 0 {
		return payload, corruptHistory(op, nil)
	}
	return payload, nil
}

func decodeEntry(record kv.KeyValueRecord, streamID string, expected EventStreamRevision) (EventHistoryEntry, error) {
	const op = "event.decode"
	var entry EventHistoryEntry
	revision, err := keyRevision(record.Item.Key(), streamID)
	if err != nil {
		return entry, err
	}
	if revision != expected {
		return entry, corruptHistory(op, nil)
	}
	recordedAt, timestampOK := record.Item.Fields["recorded_at"].(kv.KeyValueTimestamp)
	data, payloadOK := record.Item.Fields["event"].(kv.KeyValueBytes)
	if !timestampOK || recordedAt.Value().IsZero() || !payloadOK {
		return entry, corruptHistory(op, nil)
	}
	payload, err := decodePayload(data.Value())
	if err != nil {
		return entry, err
	}
	entry = EventHistoryEntry{Revision: revision, RecordedAt: recordedAt.Value(), AppendToken: payload.AppendToken, ApplicationVersion: payload.ApplicationVersion, Changes: payload.Changes}
	if value, present := record.Item.Fields["metadata"]; present {
		metadata, ok := value.(kv.KeyValueBytes)
		if !ok {
			return EventHistoryEntry{}, corruptHistory(op, nil)
		}
		entry.Metadata, err = compactJSON(metadata.Value())
		if err != nil {
			return EventHistoryEntry{}, corruptHistory(op, err)
		}
	}
	return entry, nil
}

func sameAppend(entry EventHistoryEntry, proposed kv.KeyValueItem) bool {
	payload, err := json.Marshal(eventPayload{eventFormatVersion, entry.ApplicationVersion, entry.AppendToken, entry.Changes})
	if err != nil || !bytes.Equal(payload, proposed.Fields["event"].(kv.KeyValueBytes).Value()) {
		return false
	}
	metadata, present := proposed.Fields["metadata"].(kv.KeyValueBytes)
	return (len(entry.Metadata) != 0) == present && bytes.Equal(entry.Metadata, metadata.Value())
}
