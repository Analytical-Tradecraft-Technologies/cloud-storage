package kv

import (
	"encoding/binary"
	"math"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
)

const maxDocumentDepth = 64

func invalidDocument() error {
	// Never include user field names or contents in errors.
	return &providercontracts.StorageError{Kind: providercontracts.ErrInvalidArgument, Operation: "kv.document.codec"}
}

// MarshalBinary returns the deterministic, versioned KVD1 binary encoding.
// It preserves logical types, exact integers, float bits and nanosecond UTC
// timestamps. Nil and empty containers encode identically. Cycles, excessive
// nesting, invalid UTF-8, non-finite floats and out-of-range dates are rejected
// with providercontracts.ErrInvalidArgument. Providers may use this as a fallback when
// native attributes cannot preserve a field's semantics. Encoded fields are
// not necessarily queryable as native backend attributes.
func (d KeyValueDocument) MarshalBinary() ([]byte, error) {
	return appendField([]byte("KVD1"), Document(d), 0)
}

func appendSized(dst []byte, data []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(data)))
	return append(dst, data...)
}

func appendField(dst []byte, v KeyValueFieldValue, depth int) ([]byte, error) {
	if v == nil {
		v = Null()
	}
	switch value := v.(type) {
	case KeyValueNull:
		dst = append(dst, byte(FieldNull))
	case KeyValueBool:
		dst = append(dst, byte(FieldBool), 0)
		if value {
			dst[len(dst)-1] = 1
		}
	case KeyValueInt64:
		dst = append(dst, byte(FieldInt64))
		dst = binary.LittleEndian.AppendUint64(dst, uint64(value))
	case KeyValueFloat64:
		f := value.Value()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, invalidDocument()
		}
		dst = append(dst, byte(FieldFloat64))
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(f))
	case KeyValueString:
		if !utf8.ValidString(value.Value()) {
			return nil, invalidDocument()
		}
		dst = append(dst, byte(FieldString))
		dst = appendSized(dst, []byte(value))
	case KeyValueBytes:
		dst = append(dst, byte(FieldBytes))
		dst = appendSized(dst, value)
	case KeyValueTimestamp:
		t := value.Value()
		if t.Year() < 1 || t.Year() > 9999 {
			return nil, invalidDocument()
		}
		dst = append(dst, byte(FieldTimestamp))
		dst = binary.LittleEndian.AppendUint64(dst, uint64(t.Unix()))
		dst = binary.LittleEndian.AppendUint32(dst, uint32(t.Nanosecond()))
	case KeyValueList:
		if depth >= maxDocumentDepth {
			return nil, invalidDocument()
		}
		dst = append(dst, byte(FieldList))
		dst = binary.AppendUvarint(dst, uint64(len(value)))
		for _, child := range value {
			var err error
			dst, err = appendField(dst, child, depth+1)
			if err != nil {
				return nil, err
			}
		}
	case KeyValueDocumentField:
		if depth >= maxDocumentDepth {
			return nil, invalidDocument()
		}
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		dst = append(dst, byte(FieldDocument))
		dst = binary.AppendUvarint(dst, uint64(len(keys)))
		for _, key := range keys {
			if !utf8.ValidString(key) {
				return nil, invalidDocument()
			}
			dst = appendSized(dst, []byte(key))
			var err error
			dst, err = appendField(dst, value[key], depth+1)
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, invalidDocument()
	}
	return dst, nil
}

// UnmarshalBinary decodes KVD1 into independently owned fields. Unknown formats,
// invalid values, duplicate names and trailing bytes fail with ErrInvalidArgument.
// The receiver is unchanged on error. Decoding bounds container allocations by
// the input size and enforces the same depth limit as encoding.
func (d *KeyValueDocument) UnmarshalBinary(data []byte) error {
	if d == nil || len(data) < 4 || string(data[:4]) != "KVD1" {
		return invalidDocument()
	}
	r := documentReader{data: data[4:]}
	v, err := r.field(0)
	if err != nil {
		return err
	}
	doc, ok := v.(KeyValueDocumentField)
	if !ok || len(r.data) != 0 {
		return invalidDocument()
	}
	*d = doc.Value()
	return nil
}

type documentReader struct{ data []byte }

func (r *documentReader) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.data) {
		return nil, invalidDocument()
	}
	b := r.data[:n]
	r.data = r.data[n:]
	return b, nil
}
func (r *documentReader) count() (int, error) {
	n, size := binary.Uvarint(r.data)
	if size <= 0 {
		return 0, invalidDocument()
	}
	r.data = r.data[size:]
	if n > uint64(len(r.data)) {
		return 0, invalidDocument()
	}
	return int(n), nil
}
func (r *documentReader) sized() ([]byte, error) {
	n, err := r.count()
	if err != nil {
		return nil, err
	}
	return r.take(n)
}
func (r *documentReader) field(depth int) (KeyValueFieldValue, error) {
	tag, err := r.take(1)
	if err != nil {
		return Null(), err
	}
	kind := KeyValueFieldKind(tag[0])
	switch kind {
	case FieldNull:
		return Null(), nil
	case FieldBool:
		b, err := r.take(1)
		if err != nil || b[0] > 1 {
			return Null(), invalidDocument()
		}
		return Bool(b[0] == 1), nil
	case FieldInt64, FieldFloat64:
		b, err := r.take(8)
		if err != nil {
			return Null(), err
		}
		bits := binary.LittleEndian.Uint64(b)
		if kind == FieldInt64 {
			return Int64(int64(bits)), nil
		}
		f := math.Float64frombits(bits)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return Null(), invalidDocument()
		}
		return Float64(f), nil
	case FieldString, FieldBytes:
		b, err := r.sized()
		if err != nil {
			return Null(), err
		}
		if kind == FieldBytes {
			return Bytes(append([]byte(nil), b...)), nil
		}
		if !utf8.Valid(b) {
			return Null(), invalidDocument()
		}
		return String(string(b)), nil
	case FieldTimestamp:
		b, err := r.take(12)
		if err != nil {
			return Null(), err
		}
		sec := int64(binary.LittleEndian.Uint64(b))
		ns := binary.LittleEndian.Uint32(b[8:])
		if sec < -62135596800 || sec > 253402300799 || ns >= 1000000000 {
			return Null(), invalidDocument()
		}
		return Timestamp(time.Unix(sec, int64(ns))), nil
	case FieldList, FieldDocument:
		if depth >= maxDocumentDepth {
			return Null(), invalidDocument()
		}
		count, err := r.count()
		if err != nil {
			return Null(), err
		}
		if kind == FieldList {
			list := make([]KeyValueFieldValue, count)
			for i := range list {
				list[i], err = r.field(depth + 1)
				if err != nil {
					return Null(), err
				}
			}
			return List(list...), nil
		}
		// Each document entry requires at least a name length and a type tag.
		if count > len(r.data)/2 {
			return Null(), invalidDocument()
		}
		doc := make(KeyValueDocument, count)
		for i := 0; i < count; i++ {
			key, err := r.sized()
			if err != nil {
				return Null(), err
			}
			if !utf8.Valid(key) {
				return Null(), invalidDocument()
			}
			name := string(key)
			if _, exists := doc[name]; exists {
				return Null(), invalidDocument()
			}
			doc[name], err = r.field(depth + 1)
			if err != nil {
				return Null(), err
			}
		}
		return Document(doc), nil
	default:
		return Null(), invalidDocument()
	}
}
