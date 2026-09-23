package eventsourcing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// This test adapter implements only the generic contract. In particular, it
// offers no transactions, scans, AWS APIs or mutable head-record convention.
type memoryKV struct {
	mu          sync.Mutex
	rows        map[kv.KeyValueKey]kv.KeyValueRecord
	createCalls int
	readCalls   int
	pageCap     int
	createError error // returned once after a successful commit
	queryError  error
	queryHook   func(kv.KeyValueQuery) (kv.KeyValueQueryPage, error)
}

func newMemoryKV() *memoryKV {
	return &memoryKV{rows: make(map[kv.KeyValueKey]kv.KeyValueRecord)}
}

func cloneRecord(record kv.KeyValueRecord) kv.KeyValueRecord {
	data, err := record.Item.Fields.MarshalBinary()
	if err != nil {
		panic(err)
	}
	var fields kv.KeyValueDocument
	if err := fields.UnmarshalBinary(data); err != nil {
		panic(err)
	}
	record.Item.Fields = fields
	return record
}

func (m *memoryKV) Get(ctx context.Context, key kv.KeyValueKey) (kv.KeyValueRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readCalls++
	if err := ctx.Err(); err != nil {
		return kv.KeyValueRecord{}, err
	}
	row, ok := m.rows[key]
	if !ok {
		return kv.KeyValueRecord{}, &contracts.StorageError{Kind: contracts.ErrNotFound}
	}
	return cloneRecord(row), nil
}

func (m *memoryKV) Create(ctx context.Context, item kv.KeyValueItem) (kv.KeyValueVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createCalls++
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if _, exists := m.rows[item.Key()]; exists {
		return "", &contracts.StorageError{Kind: contracts.ErrAlreadyExists}
	}
	version := kv.KeyValueVersion(strconv.Itoa(m.createCalls))
	m.rows[item.Key()] = cloneRecord(kv.KeyValueRecord{Item: item, Version: version, LastModifiedAt: time.Now().UTC()})
	if m.createError != nil {
		err := m.createError
		m.createError = nil
		return "", err
	}
	return version, nil
}

func (m *memoryKV) QueryPartition(ctx context.Context, q kv.KeyValueQuery) (kv.KeyValueQueryPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readCalls++
	if err := ctx.Err(); err != nil {
		return kv.KeyValueQueryPage{}, err
	}
	if m.queryHook != nil {
		return m.queryHook(q)
	}
	if m.queryError != nil {
		return kv.KeyValueQueryPage{}, m.queryError
	}
	var rows []kv.KeyValueRecord
	for key, row := range m.rows {
		if key.PartitionKey == q.PartitionKey && strings.HasPrefix(key.SortKey, q.SortKeyPrefix) && key.SortKey > q.PageToken {
			rows = append(rows, cloneRecord(row))
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Item.SortKey < rows[j].Item.SortKey })
	limit := q.PageSize
	if limit == 0 {
		limit = 100
	}
	if m.pageCap > 0 && m.pageCap < limit {
		limit = m.pageCap
	}
	page := kv.KeyValueQueryPage{}
	if len(rows) > limit {
		rows = rows[:limit]
		page.NextPageToken = rows[len(rows)-1].Item.SortKey
	}
	page.Records = rows
	return page, nil
}

func (*memoryKV) Replace(context.Context, kv.KeyValueItem, kv.KeyValueVersion) (kv.KeyValueVersion, error) {
	panic("event sourcing must never replace a row")
}

func (*memoryKV) Delete(context.Context, kv.KeyValueKey, kv.KeyValueVersion) error {
	panic("event sourcing must never delete a row")
}

func newCounterStore(t *testing.T, backend kv.KeyValueStore) *EventStreamStore[int64] {
	t.Helper()
	store, err := NewEventStreamStore(backend, func() int64 { return 0 }, func(_ context.Context, value int64, changes []EventChange) (int64, error) {
		for _, change := range changes {
			var amount int64
			if err := json.Unmarshal(change.Data, &amount); err != nil {
				return 0, err
			}
			switch change.Version {
			case 0:
				value += amount
			case 1:
				value -= amount
			default:
				return 0, fmt.Errorf("unknown application version")
			}
		}
		return value, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func appendNumbers(token string, version uint64, numbers ...int64) EventAppend {
	request := EventAppend{AppendToken: token, ApplicationVersion: version}
	for _, n := range numbers {
		request.Changes = append(request.Changes, json.RawMessage(strconv.FormatInt(n, 10)))
	}
	return request
}

func mustAppend(t *testing.T, store *EventStreamStore[int64], stream string, expected EventStreamRevision, request EventAppend) {
	t.Helper()
	if revision, err := store.TryAppend(context.Background(), stream, expected, request); err != nil || revision != expected+1 {
		t.Fatalf("append revision %d: got %d, %v", expected+1, revision, err)
	}
}

func TestStateReplaysVersionsAndFlattensRowsAcrossPages(t *testing.T) {
	backend := newMemoryKV()
	backend.pageCap = 2
	writer := newCounterStore(t, backend)
	mustAppend(t, writer, "account", 0, appendNumbers("first", 0, 10, 20))
	mustAppend(t, writer, "account", 1, appendNumbers("second", 1, 5))
	mustAppend(t, writer, "account", 2, appendNumbers("third", 0, 9007199254740993))
	mustAppend(t, writer, "unrelated", 0, appendNumbers("first", 0, 999))
	var callbackSizes []int
	reader, err := NewEventStreamStore(backend, func() []EventChange { return nil }, func(_ context.Context, state []EventChange, changes []EventChange) ([]EventChange, error) {
		callbackSizes = append(callbackSizes, len(changes))
		return append(state, changes...), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.ReadState(context.Background(), "account")
	if err != nil || result.Revision != 3 || !reflect.DeepEqual(callbackSizes, []int{3, 1}) {
		t.Fatalf("rows were not flattened across pages: %+v %v callbacks %v", result, err, callbackSizes)
	}
	if result.Value[2].Version != 1 || string(result.Value[3].Data) != "9007199254740993" {
		t.Fatal("lost application version or JSON integer precision")
	}
	state, err := writer.ReadState(context.Background(), "account")
	if err != nil || state.Value != 9007199254741018 || state.Revision != 3 {
		t.Fatalf("mixed-version state: %+v %v", state, err)
	}
	missing, err := writer.ReadState(context.Background(), "missing")
	if err != nil || missing.Value != 0 || missing.Revision != 0 {
		t.Fatalf("missing stream must be initial state: %+v %v", missing, err)
	}
}

func TestConcurrentWritersHaveExactlyOneWinner(t *testing.T) {
	backend := newMemoryKV()
	store := newCounterStore(t, backend)
	const writers = 32
	start := make(chan struct{})
	errorsByWriter := make(chan error, writers)
	for i := range writers {
		go func(i int) {
			<-start
			_, err := store.TryAppend(context.Background(), "stream", 0, appendNumbers(strconv.Itoa(i), 0, 1, 2))
			errorsByWriter <- err
		}(i)
	}
	close(start)
	winners := 0
	for range writers {
		if err := <-errorsByWriter; err == nil {
			winners++
		} else if !errors.Is(err, contracts.ErrConflict) || !errors.Is(err, contracts.ErrAlreadyExists) {
			t.Fatalf("unexpected loser error: %v", err)
		}
	}
	if winners != 1 || len(backend.rows) != 1 {
		t.Fatalf("winners=%d rows=%d", winners, len(backend.rows))
	}
	state, err := store.ReadState(context.Background(), "stream")
	if err != nil || state.Value != 3 || state.Revision != 1 {
		t.Fatalf("atomic batch was split: %+v %v", state, err)
	}
	for _, expected := range []EventStreamRevision{0, 2, 99} {
		if _, err := store.TryAppend(context.Background(), "stream", expected, appendNumbers("other", 0, 4)); !errors.Is(err, contracts.ErrConflict) {
			t.Fatalf("stale/future revision accepted: %d %v", expected, err)
		}
	}
	mustAppend(t, store, "stream", 1, appendNumbers("next", 0, 4))
}

func TestLostAcknowledgementReconcilesOnlyIdenticalAppend(t *testing.T) {
	backend := newMemoryKV()
	cause := errors.New("transport dropped")
	backend.createError = &contracts.StorageError{Kind: contracts.ErrUnavailable, Cause: cause, OutcomeUnknown: true}
	store := newCounterStore(t, backend)
	request := appendNumbers("operation-id", 0, 2, 3)
	request.Metadata = json.RawMessage(`{ "service": "example", "version": "1.2" }`)
	if revision, err := store.TryAppend(context.Background(), "stream", 0, request); revision != 0 || !errors.Is(err, contracts.ErrOutcomeUnknown) || !errors.Is(err, cause) || !errors.Is(err, contracts.ErrUnavailable) || backend.createCalls != 1 {
		t.Fatalf("uncertain write was hidden/retried: %d %v calls=%d", revision, err, backend.createCalls)
	}
	first, _ := backend.Get(context.Background(), eventKey("stream", 1))
	request.Changes[0] = json.RawMessage(" 2 ")
	request.Metadata = json.RawMessage(`{"service":"example","version":"1.2"}`)
	mustAppend(t, store, "stream", 0, request)
	mustAppend(t, store, "stream", 1, appendNumbers("next", 0, 9))
	mustAppend(t, store, "stream", 0, request) // recovery still works after later appends
	last, _ := backend.Get(context.Background(), eventKey("stream", 1))
	if !reflect.DeepEqual(first, last) || len(backend.rows) != 2 {
		t.Fatal("reconciliation rewrote history or timestamp")
	}
	for _, changed := range []EventAppend{
		{AppendToken: "another-operation", Changes: request.Changes, Metadata: request.Metadata},
		{AppendToken: request.AppendToken, ApplicationVersion: 1, Changes: request.Changes, Metadata: request.Metadata},
		{AppendToken: request.AppendToken, Changes: []json.RawMessage{json.RawMessage("99")}, Metadata: request.Metadata},
		{AppendToken: request.AppendToken, Changes: request.Changes},
	} {
		if _, err := store.TryAppend(context.Background(), "stream", 0, changed); !errors.Is(err, contracts.ErrConflict) {
			t.Fatalf("different append was treated as a retry: %v", err)
		}
	}
}

func TestHistoryIsExplicitOwnedAndPaged(t *testing.T) {
	backend := newMemoryKV()
	store := newCounterStore(t, backend)
	request := appendNumbers("one", 0, 1, 2)
	request.Metadata = json.RawMessage(`{"service":"counter","version":"v3"}`)
	before := time.Now().UTC()
	mustAppend(t, store, "stream", 0, request)
	mustAppend(t, store, "stream", 1, appendNumbers("two", 1, 3))
	first, err := store.ReadHistory(context.Background(), "stream", EventHistoryQuery{PageSize: 1})
	if err != nil || len(first.Entries) != 1 || first.NextPageToken == "" {
		t.Fatalf("first page: %+v %v", first, err)
	}
	entry := first.Entries[0]
	if entry.Revision != 1 || entry.AppendToken != "one" || len(entry.Changes) != 2 || entry.RecordedAt.Before(before) || entry.RecordedAt.Location() != time.UTC || string(entry.Metadata) != string(request.Metadata) {
		t.Fatalf("history metadata: %+v", entry)
	}
	entry.Changes[0][0] = '9'
	entry.Metadata[0] = '['
	state, err := store.ReadState(context.Background(), "stream")
	if err != nil || state.Value != 0 || state.Revision != 2 {
		t.Fatalf("caller modified stored buffers: %+v %v", state, err)
	}
	// New instances can continue an old instance's token.
	second, err := newCounterStore(t, backend).ReadHistory(context.Background(), "stream", EventHistoryQuery{PageSize: 1, PageToken: first.NextPageToken})
	if err != nil || len(second.Entries) != 1 || second.NextPageToken != "" || second.Entries[0].Revision != 2 || second.Entries[0].Metadata != nil {
		t.Fatalf("continuation: %+v %v", second, err)
	}
	for _, q := range []EventHistoryQuery{{PageSize: 2, PageToken: first.NextPageToken}, {PageSize: 1, PageToken: "!"}, {PageSize: 1001}, {PageSize: -1}, {PageToken: strings.Repeat("a", 256*1024+1)}} {
		reads := backend.readCalls
		if _, err := store.ReadHistory(context.Background(), "stream", q); !errors.Is(err, contracts.ErrInvalidArgument) || backend.readCalls != reads {
			t.Fatalf("invalid history query reached backend: %v", err)
		}
	}
	reads := backend.readCalls
	if _, err := store.ReadHistory(context.Background(), "other", EventHistoryQuery{PageSize: 1, PageToken: first.NextPageToken}); !errors.Is(err, contracts.ErrInvalidArgument) || backend.readCalls != reads {
		t.Fatal("cross-stream cursor reached backend")
	}
}

func TestEmptyIntermediatePagesAndCursorLoops(t *testing.T) {
	backend := newMemoryKV()
	store := newCounterStore(t, backend)
	mustAppend(t, store, "stream", 0, appendNumbers("one", 0, 3))
	row := backend.rows[eventKey("stream", 1)]
	calls := 0
	backend.queryHook = func(q kv.KeyValueQuery) (kv.KeyValueQueryPage, error) {
		calls++
		if calls == 1 {
			return kv.KeyValueQueryPage{NextPageToken: "empty-page"}, nil
		}
		if q.PageToken != "empty-page" {
			t.Fatal("lost provider continuation")
		}
		return kv.KeyValueQueryPage{Records: []kv.KeyValueRecord{cloneRecord(row)}}, nil
	}
	state, err := store.ReadState(context.Background(), "stream")
	if err != nil || state.Value != 3 || state.Revision != 1 || calls != 2 {
		t.Fatalf("empty page terminated replay: %+v %v calls=%d", state, err, calls)
	}
	backend.queryHook = func(q kv.KeyValueQuery) (kv.KeyValueQueryPage, error) {
		if q.PageToken == "A" {
			return kv.KeyValueQueryPage{NextPageToken: "B"}, nil
		}
		return kv.KeyValueQueryPage{NextPageToken: "A"}, nil
	}
	if _, err := store.ReadState(context.Background(), "stream"); !errors.Is(err, ErrCorruptHistory) {
		t.Fatalf("cursor cycle accepted: %v", err)
	}
}

func TestInvalidInputDoesNotWrite(t *testing.T) {
	backend := newMemoryKV()
	store := newCounterStore(t, backend)
	for _, request := range []EventAppend{
		{}, {AppendToken: "id"}, {Changes: []json.RawMessage{json.RawMessage("1")}},
		{AppendToken: "id", Changes: []json.RawMessage{nil}},
		{AppendToken: "id", Changes: []json.RawMessage{json.RawMessage("{")}},
		{AppendToken: "id", Changes: []json.RawMessage{json.RawMessage{'"', 0xff, '"'}}},
		{AppendToken: "id", Changes: []json.RawMessage{json.RawMessage("1")}, Metadata: json.RawMessage("broken")},
	} {
		if _, err := store.TryAppend(context.Background(), "stream", 0, request); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatalf("invalid append accepted: %v", err)
		}
	}
	for _, stream := range []string{"", "\xff"} {
		if _, err := store.TryAppend(context.Background(), stream, 0, appendNumbers("id", 0, 1)); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatal("invalid stream accepted")
		}
		if _, err := store.ReadState(context.Background(), stream); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatal("invalid state stream accepted")
		}
	}
	if _, err := store.TryAppend(context.Background(), "stream", EventStreamRevision(math.MaxUint64), appendNumbers("id", 0, 1)); !errors.Is(err, contracts.ErrInvalidArgument) {
		t.Fatal("revision overflow accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.TryAppend(ctx, "stream", 0, appendNumbers("id", 0, 1)); !errors.Is(err, context.Canceled) || errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatalf("pre-I/O cancellation: %v", err)
	}
	if backend.createCalls != 0 || backend.readCalls != 0 {
		t.Fatal("invalid calls performed I/O")
	}
}

func TestCorruptAndUnsupportedHistoryCannotProduceState(t *testing.T) {
	for name, damage := range map[string]func(*memoryKV){
		"missing first":     func(m *memoryKV) { delete(m.rows, eventKey("stream", 1)) },
		"missing middle":    func(m *memoryKV) { delete(m.rows, eventKey("stream", 2)) },
		"invalid JSON":      func(m *memoryKV) { m.rows[eventKey("stream", 2)].Item.Fields["event"] = kv.Bytes([]byte("{")) },
		"missing timestamp": func(m *memoryKV) { delete(m.rows[eventKey("stream", 2)].Item.Fields, "recorded_at") },
		"invalid metadata":  func(m *memoryKV) { m.rows[eventKey("stream", 2)].Item.Fields["metadata"] = kv.Bytes([]byte("bad")) },
		"unsupported envelope": func(m *memoryKV) {
			m.rows[eventKey("stream", 2)].Item.Fields["event"] = kv.Bytes([]byte(`{"format_version":2}`))
		},
		"duplicate envelope field": func(m *memoryKV) {
			m.rows[eventKey("stream", 2)].Item.Fields["event"] = kv.Bytes([]byte(`{"format_version":1,"format_version":1,"application_version":0,"append_token":"x","changes":[1]}`))
		},
		"missing application version": func(m *memoryKV) {
			m.rows[eventKey("stream", 2)].Item.Fields["event"] = kv.Bytes([]byte(`{"format_version":1,"append_token":"x","changes":[1]}`))
		},
		"null application version": func(m *memoryKV) {
			m.rows[eventKey("stream", 2)].Item.Fields["event"] = kv.Bytes([]byte(`{"format_version":1,"application_version":null,"append_token":"x","changes":[1]}`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			backend := newMemoryKV()
			backend.pageCap = 1
			store := newCounterStore(t, backend)
			for i := range 3 {
				mustAppend(t, store, "stream", EventStreamRevision(i), appendNumbers(strconv.Itoa(i), 0, 7))
			}
			damage(backend)
			state, err := store.ReadState(context.Background(), "stream")
			want := error(ErrCorruptHistory)
			if name == "unsupported envelope" {
				want = contracts.ErrUnsupported
			}
			if !errors.Is(err, want) || state.Value != 0 || state.Revision != 0 {
				t.Fatalf("corrupt history yielded partial state: %+v %v", state, err)
			}
		})
	}
}

func TestBuilderAndProviderErrorsPreserveCause(t *testing.T) {
	backend := newMemoryKV()
	store := newCounterStore(t, backend)
	mustAppend(t, store, "stream", 0, appendNumbers("one", 0, 5))
	cause := errors.New("private builder details")
	reader, _ := NewEventStreamStore(backend, func() int64 { return 1 }, func(context.Context, int64, []EventChange) (int64, error) { return 999, cause })
	if state, err := reader.ReadState(context.Background(), "stream"); state.Value != 0 || state.Revision != 0 || !errors.Is(err, cause) || strings.Contains(err.Error(), cause.Error()) {
		t.Fatalf("builder failure: %+v %v", state, err)
	}
	backend.queryError = &contracts.StorageError{Kind: contracts.ErrThrottled, Cause: cause}
	if _, err := store.ReadState(context.Background(), "stream"); !errors.Is(err, cause) || !errors.Is(err, contracts.ErrThrottled) || errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatalf("provider read failure: %v", err)
	}
	if _, err := NewEventStreamStore[int64](nil, func() int64 { return 0 }, store.build); !errors.Is(err, contracts.ErrInvalidArgument) {
		t.Fatal("nil storage accepted")
	}
}

func TestReplayMatchesApplicationModelAcrossStreamsAndRetries(t *testing.T) {
	backend := newMemoryKV()
	backend.pageCap = 7
	store := newCounterStore(t, backend)
	random := rand.New(rand.NewSource(42))
	balances := make(map[string]int64)
	revisions := make(map[string]EventStreamRevision)
	for i := range 200 {
		stream := fmt.Sprintf("account/%d", random.Intn(4))
		version := uint64(random.Intn(2))
		numbers := make([]int64, 1+random.Intn(5))
		for j := range numbers {
			numbers[j] = int64(random.Intn(1000))
			if version == 0 {
				balances[stream] += numbers[j]
			} else {
				balances[stream] -= numbers[j]
			}
		}
		request := appendNumbers(fmt.Sprintf("operation/%d", i), version, numbers...)
		if i%11 == 0 {
			backend.createError = &contracts.StorageError{Kind: contracts.ErrUnavailable, OutcomeUnknown: true}
			if _, err := store.TryAppend(context.Background(), stream, revisions[stream], request); !errors.Is(err, contracts.ErrOutcomeUnknown) {
				t.Fatalf("lost acknowledgement: %v", err)
			}
		}
		mustAppend(t, store, stream, revisions[stream], request)
		revisions[stream]++
	}
	for stream, want := range balances {
		state, err := store.ReadState(context.Background(), stream)
		if err != nil || state.Value != want || state.Revision != revisions[stream] {
			t.Fatalf("model mismatch for %s: got %+v %v want %d/%d", stream, state, err, want, revisions[stream])
		}
	}
}

func TestUncertainCreateClassificationTakesPrecedence(t *testing.T) {
	backend := newMemoryKV()
	backend.createError = &contracts.StorageError{Kind: contracts.ErrAlreadyExists, OutcomeUnknown: true}
	store := newCounterStore(t, backend)
	if _, err := store.TryAppend(context.Background(), "stream", 0, appendNumbers("one", 0, 1)); !errors.Is(err, contracts.ErrOutcomeUnknown) || errors.Is(err, contracts.ErrConflict) || backend.readCalls != 0 || backend.createCalls != 1 {
		t.Fatalf("uncertain write was reconciled as definitive conflict: %v", err)
	}
}

func TestDefaultAndExplicitHistoryPageSizeAreEquivalent(t *testing.T) {
	backend := newMemoryKV()
	backend.pageCap = 1
	store := newCounterStore(t, backend)
	mustAppend(t, store, "stream", 0, appendNumbers("one", 0, 1))
	mustAppend(t, store, "stream", 1, appendNumbers("two", 0, 2))
	page, err := store.ReadHistory(context.Background(), "stream", EventHistoryQuery{})
	if err != nil || page.NextPageToken == "" {
		t.Fatal(err)
	}
	page, err = store.ReadHistory(context.Background(), "stream", EventHistoryQuery{PageSize: 100, PageToken: page.NextPageToken})
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Revision != 2 {
		t.Fatalf("normalized size invalidated token: %+v %v", page, err)
	}
}

func FuzzDecodeEventEnvelope(f *testing.F) {
	for _, seed := range []string{
		`{"format_version":1,"application_version":0,"append_token":"a","changes":[1,null,{"id":9007199254740993}]}`,
		`{"format_version":2}`, `null`, `{}`, `{"format_version":1,"format_version":2}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		payload, err := decodePayload(data)
		if err != nil {
			return
		}
		if payload.FormatVersion != 1 || payload.AppendToken == "" || len(payload.Changes) == 0 {
			t.Fatal("accepted invalid envelope")
		}
		for _, change := range payload.Changes {
			if !json.Valid(change) {
				t.Fatal("accepted invalid change")
			}
		}
	})
}
