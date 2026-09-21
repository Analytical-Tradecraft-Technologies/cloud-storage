package kv

import "time"

// KeyValueDocument is a schema-free collection of typed fields. Names and
// strings must be valid UTF-8; empty names are allowed. Missing fields differ
// from explicit Null values. Nil field values mean Null. Nil and empty
// containers are equivalent. Documents must be acyclic, at most 64 containers
// deep, with finite floats and timestamp years 1 through 9999. Backend size
// limits still apply. Providers must preserve logical types, using the shared
// encoding when native attributes cannot do so.
type KeyValueDocument map[string]KeyValueFieldValue

// KeyValueFieldKind identifies a field's logical type.
type KeyValueFieldKind uint8

const (
	FieldNull KeyValueFieldKind = iota
	FieldBool
	FieldString
	FieldInt64
	FieldFloat64
	FieldBytes
	FieldTimestamp
	FieldList
	FieldDocument
)

// KeyValueFieldValue is a closed set of portable field types. Use constructors
// such as Int64 and String when writing, and concrete type assertions when
// reading. Each concrete type has a Value method returning its typed payload.
// Constructors and Value methods borrow maps, slices and bytes without copying;
// callers must not mutate these during store or encoding calls. Use the
// non-pointer concrete values supplied by this package.
type KeyValueFieldValue interface {
	Kind() KeyValueFieldKind
	keyValueField()
}

// KeyValueNull represents an explicit null.
type KeyValueNull struct{}

// Null constructs an explicit null field.
func Null() KeyValueNull { return KeyValueNull{} }

// Kind returns FieldNull.
func (KeyValueNull) Kind() KeyValueFieldKind { return FieldNull }
func (KeyValueNull) keyValueField()          {}

// KeyValueBool is a boolean field.
type KeyValueBool bool

// Bool constructs a boolean field.
func Bool(v bool) KeyValueBool { return KeyValueBool(v) }

// Kind returns FieldBool.
func (KeyValueBool) Kind() KeyValueFieldKind { return FieldBool }
func (KeyValueBool) keyValueField()          {}

// Value returns the typed payload without copying.
func (v KeyValueBool) Value() bool { return bool(v) }

// KeyValueString is a UTF-8 string field.
type KeyValueString string

// String constructs a UTF-8 string field.
func String(v string) KeyValueString { return KeyValueString(v) }

// Kind returns FieldString.
func (KeyValueString) Kind() KeyValueFieldKind { return FieldString }
func (KeyValueString) keyValueField()          {}

// Value returns the typed payload without copying.
func (v KeyValueString) Value() string { return string(v) }

// KeyValueInt64 is a exact signed 64-bit integer field.
type KeyValueInt64 int64

// Int64 constructs a exact signed 64-bit integer field.
func Int64(v int64) KeyValueInt64 { return KeyValueInt64(v) }

// Kind returns FieldInt64.
func (KeyValueInt64) Kind() KeyValueFieldKind { return FieldInt64 }
func (KeyValueInt64) keyValueField()          {}

// Value returns the typed payload without copying.
func (v KeyValueInt64) Value() int64 { return int64(v) }

// KeyValueFloat64 is a finite floating-point number field.
type KeyValueFloat64 float64

// Float64 constructs a finite floating-point number field.
func Float64(v float64) KeyValueFloat64 { return KeyValueFloat64(v) }

// Kind returns FieldFloat64.
func (KeyValueFloat64) Kind() KeyValueFieldKind { return FieldFloat64 }
func (KeyValueFloat64) keyValueField()          {}

// Value returns the typed payload without copying.
func (v KeyValueFloat64) Value() float64 { return float64(v) }

// KeyValueBytes is a binary value field.
type KeyValueBytes []byte

// Bytes constructs a binary value field.
func Bytes(v []byte) KeyValueBytes { return KeyValueBytes(v) }

// Kind returns FieldBytes.
func (KeyValueBytes) Kind() KeyValueFieldKind { return FieldBytes }
func (KeyValueBytes) keyValueField()          {}

// Value returns the typed payload without copying.
func (v KeyValueBytes) Value() []byte { return []byte(v) }

// KeyValueList is a ordered list field.
type KeyValueList []KeyValueFieldValue

// List constructs a ordered list field.
func List(v ...KeyValueFieldValue) KeyValueList { return KeyValueList(v) }

// Kind returns FieldList.
func (KeyValueList) Kind() KeyValueFieldKind { return FieldList }
func (KeyValueList) keyValueField()          {}

// Value returns the typed payload without copying.
func (v KeyValueList) Value() []KeyValueFieldValue { return []KeyValueFieldValue(v) }

// KeyValueDocumentField is a nested document field.
type KeyValueDocumentField KeyValueDocument

// Document constructs a nested document field.
func Document(v KeyValueDocument) KeyValueDocumentField { return KeyValueDocumentField(v) }

// Kind returns FieldDocument.
func (KeyValueDocumentField) Kind() KeyValueFieldKind { return FieldDocument }
func (KeyValueDocumentField) keyValueField()          {}

// Value returns the typed payload without copying.
func (v KeyValueDocumentField) Value() KeyValueDocument { return KeyValueDocument(v) }

// KeyValueTimestamp is a UTC timestamp with nanosecond precision.
// Its zero value is the beginning of year 1 UTC.
type KeyValueTimestamp struct{ value time.Time }

// Timestamp constructs a UTC timestamp, discarding monotonic clock metadata.
func Timestamp(v time.Time) KeyValueTimestamp { return KeyValueTimestamp{value: v.UTC().Round(0)} }

// Kind returns FieldTimestamp.
func (KeyValueTimestamp) Kind() KeyValueFieldKind { return FieldTimestamp }
func (KeyValueTimestamp) keyValueField()          {}

// Value returns the timestamp.
func (v KeyValueTimestamp) Value() time.Time { return v.value }
