package eventsourcing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// EventHistoryQuery selects a page of stored batches for auditing/debugging.
// PageSize defaults to 100 and must be 1..1000 when specified. Keep the stream
// and page size unchanged while following a token (zero and 100 are equivalent).
// PageToken is opaque, is not authorization, and must not be logged.
type EventHistoryQuery struct {
	PageSize  int
	PageToken string
}

// EventHistoryPage exposes row boundaries explicitly. Empty pages can have a
// continuation token; only an empty NextPageToken ends traversal. Changes during
// pagination may extend the stream. Entries and all JSON buffers are caller-owned.
type EventHistoryPage struct {
	Entries       []EventHistoryEntry
	NextPageToken string
}

type historyCursor struct {
	FormatVersion int                 `json:"v"`
	Binding       string              `json:"q"`
	After         EventStreamRevision `json:"after"`
	ProviderToken string              `json:"provider"`
}

func historyBinding(streamID string, size int) string {
	data, _ := json.Marshal(struct {
		Stream string
		Size   int
	}{streamID, size})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// ReadHistory is the explicit low-level history API. Prefer ReadState for normal
// application reads. It validates contiguous revisions rather than silently
// constructing state from a history with missing or reordered batches.
func (s *EventStreamStore[State]) ReadHistory(ctx context.Context, streamID string, query EventHistoryQuery) (EventHistoryPage, error) {
	const op = "event.read_history"
	var empty EventHistoryPage
	if !validStreamID(streamID) || query.PageSize < 0 || query.PageSize > 1000 {
		return empty, eventError(op, contracts.ErrInvalidArgument, nil)
	}
	size := query.PageSize
	if size == 0 {
		size = 100
	}
	cursor := historyCursor{FormatVersion: 1, Binding: historyBinding(streamID, size)}
	if query.PageToken != "" {
		// Bounds an untrusted envelope without imposing the AWS token format on
		// other adapters. The underlying provider remains responsible for its token.
		if len(query.PageToken) > 256*1024 {
			return empty, eventError(op, contracts.ErrInvalidArgument, nil)
		}
		data, err := base64.RawURLEncoding.DecodeString(query.PageToken)
		if err != nil || json.Unmarshal(data, &cursor) != nil {
			return empty, eventError(op, contracts.ErrInvalidArgument, err)
		}
		canonical, _ := json.Marshal(cursor)
		if !bytes.Equal(data, canonical) || cursor.FormatVersion != 1 || cursor.Binding != historyBinding(streamID, size) || cursor.ProviderToken == "" {
			return empty, eventError(op, contracts.ErrInvalidArgument, nil)
		}
	}
	if err := contextError(ctx, op); err != nil {
		return empty, err
	}
	page, err := s.storage.QueryPartition(ctx, kv.KeyValueQuery{PartitionKey: streamID, SortKeyPrefix: eventPrefix, PageSize: size, PageToken: cursor.ProviderToken})
	if err != nil {
		return empty, eventError(op, contracts.ErrUnknown, err)
	}
	if len(page.Records) > size || (page.NextPageToken != "" && page.NextPageToken == cursor.ProviderToken) {
		return empty, corruptHistory(op, nil)
	}
	result := EventHistoryPage{Entries: make([]EventHistoryEntry, 0, len(page.Records))}
	for _, record := range page.Records {
		if cursor.After == EventStreamRevision(math.MaxUint64) {
			return empty, corruptHistory(op, nil)
		}
		entry, err := decodeEntry(record, streamID, cursor.After+1)
		if err != nil {
			return empty, err
		}
		result.Entries = append(result.Entries, entry)
		cursor.After++
	}
	if page.NextPageToken != "" {
		cursor.ProviderToken = page.NextPageToken
		data, _ := json.Marshal(cursor)
		result.NextPageToken = base64.RawURLEncoding.EncodeToString(data)
		if len(result.NextPageToken) > 256*1024 {
			return empty, eventError(op, contracts.ErrUnsupported, nil)
		}
	}
	return result, nil
}
