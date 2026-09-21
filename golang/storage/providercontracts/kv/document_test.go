package kv_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

func TestDocumentRoundTrip(t *testing.T) {
	stamp := time.Date(2026, 9, 21, 12, 34, 56, 123456789, time.FixedZone("offset", 3600))
	doc := kv.KeyValueDocument{
		"min": kv.Int64(math.MinInt64), "max": kv.Int64(math.MaxInt64),
		"float": kv.Float64(42), "negativeZero": kv.Float64(math.Copysign(0, -1)),
		"text": kv.String("42"), "binary": kv.Bytes([]byte{0, 255}),
		"time": kv.Timestamp(stamp), "null": kv.Null(), "bool": kv.Bool(true),
		"nested": kv.List(kv.List(kv.Int64(1)), kv.Document(kv.KeyValueDocument{"x": kv.String("你好")})),
	}
	encoded, err := doc.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var decoded kv.KeyValueDocument
	if err := decoded.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	if decoded["min"].(kv.KeyValueInt64).Value() != math.MinInt64 || decoded["max"].(kv.KeyValueInt64).Value() != math.MaxInt64 {
		t.Fatal("integer precision lost")
	}
	if decoded["float"].Kind() != kv.FieldFloat64 || decoded["text"].Kind() != kv.FieldString {
		t.Fatal("types lost")
	}
	if !math.Signbit(decoded["negativeZero"].(kv.KeyValueFloat64).Value()) {
		t.Fatal("negative zero lost")
	}
	gotTime := decoded["time"].(kv.KeyValueTimestamp).Value()
	if !gotTime.Equal(stamp) || gotTime.Location() != time.UTC {
		t.Fatal("timestamp not preserved in UTC")
	}
	if _, ok := decoded["missing"]; ok {
		t.Fatal("missing field present")
	}
	if decoded["null"].Kind() != kv.FieldNull {
		t.Fatal("null lost")
	}
	again, err := decoded.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, again) {
		t.Fatal("unstable encoding")
	}
	for i := range encoded {
		encoded[i] = 0
	}
	if !bytes.Equal(decoded["binary"].(kv.KeyValueBytes).Value(), []byte{0, 255}) {
		t.Fatal("decoded bytes alias input")
	}
}

func TestEncodingGolden(t *testing.T) {
	// KVD1, document tag, one field, name a, int64 tag, little endian 42.
	want, _ := hex.DecodeString("4b56443108010161032a00000000000000")
	got, err := (kv.KeyValueDocument{"a": kv.Int64(42)}).MarshalBinary()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("got %x, error %v", got, err)
	}
	var decoded kv.KeyValueDocument
	if err := decoded.UnmarshalBinary(want); err != nil {
		t.Fatal(err)
	}
	if decoded["a"].(kv.KeyValueInt64).Value() != 42 {
		t.Fatal("golden decode failed")
	}
	for n := 0; n < len(want); n++ {
		if err := decoded.UnmarshalBinary(want[:n]); !errors.Is(err, providercontracts.ErrInvalidArgument) {
			t.Fatalf("accepted truncation at %d", n)
		}
	}
}

func TestRejectInvalidDocuments(t *testing.T) {
	cycle := kv.KeyValueDocument{}
	cycle["self"] = kv.Document(cycle)
	var pointer *kv.KeyValueInt64
	cases := []kv.KeyValueDocument{
		{"x": kv.Float64(math.NaN())}, {"x": kv.Float64(math.Inf(1))},
		{"x": kv.String(string([]byte{255}))}, {string([]byte{255}): kv.Null()},
		{"x": kv.Timestamp(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))},
		{"x": pointer}, cycle,
	}
	for i, doc := range cases {
		if _, err := doc.MarshalBinary(); !errors.Is(err, providercontracts.ErrInvalidArgument) {
			t.Fatalf("case %d: %v", i, err)
		}
	}
}

func TestRejectMalformedEncoding(t *testing.T) {
	valid, _ := (kv.KeyValueDocument{}).MarshalBinary()
	cases := [][]byte{
		[]byte("KVD2"), append(append([]byte{}, valid...), 0),
		append([]byte("KVD1"), 255),
		// Duplicate empty field name.
		append([]byte("KVD1"), 8, 2, 0, 0, 0, 0),
		// Invalid bool, unknown type, unreasonable container count.
		append([]byte("KVD1"), 8, 1, 0, 1, 2),
		append([]byte("KVD1"), 8, 1, 0, 255),
		append([]byte("KVD1"), 8, 255, 255, 255, 255, 15),
	}
	for i, data := range cases {
		doc := kv.KeyValueDocument{"keep": kv.Int64(1)}
		if err := doc.UnmarshalBinary(data); !errors.Is(err, providercontracts.ErrInvalidArgument) {
			t.Fatalf("case %d: %v", i, err)
		}
		if doc["keep"].(kv.KeyValueInt64).Value() != 1 {
			t.Fatal("receiver changed on error")
		}
	}
}

func TestNilContainersAndDepth(t *testing.T) {
	a := kv.KeyValueDocument{"n": nil, "list": kv.List(), "doc": kv.Document(nil), "bytes": kv.Bytes(nil)}
	b := kv.KeyValueDocument{"n": kv.Null(), "list": kv.List([]kv.KeyValueFieldValue{}...), "doc": kv.Document(kv.KeyValueDocument{}), "bytes": kv.Bytes([]byte{})}
	x, _ := a.MarshalBinary()
	y, _ := b.MarshalBinary()
	if !bytes.Equal(x, y) {
		t.Fatal("nil and empty differ")
	}
	var v kv.KeyValueFieldValue = kv.Null()
	for i := 0; i < 63; i++ {
		v = kv.List(v)
	}
	doc := kv.KeyValueDocument{"v": v}
	encoded, err := doc.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var decoded kv.KeyValueDocument
	if err := decoded.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	doc["v"] = kv.List(v)
	if _, err := doc.MarshalBinary(); !errors.Is(err, providercontracts.ErrInvalidArgument) {
		t.Fatal("excessive depth accepted")
	}
}

func FuzzDocumentDecode(f *testing.F) {
	seed, _ := (kv.KeyValueDocument{"answer": kv.Int64(42)}).MarshalBinary()
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		var doc kv.KeyValueDocument
		if err := doc.UnmarshalBinary(data); err != nil {
			return
		}
		encoded, err := doc.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		var decoded kv.KeyValueDocument
		if err := decoded.UnmarshalBinary(encoded); err != nil {
			t.Fatal(err)
		}
	})
}
