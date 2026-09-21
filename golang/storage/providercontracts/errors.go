// Package providercontracts defines errors shared by the kv and blob provider contracts.
// It has no cloud SDK dependencies and performs no storage operations itself.
package providercontracts

// StorageErrorKind is a stable classification usable with errors.Is.
// Providers should return *StorageError to attach operation context and a cause.
type StorageErrorKind string

const (
	// ErrUnknown means no more specific classification is available.
	ErrUnknown StorageErrorKind = "unknown"
	// ErrNotFound means a read found no record or blob at the requested key.
	ErrNotFound StorageErrorKind = "not found"
	// ErrAlreadyExists means Create found an existing key without changing it.
	ErrAlreadyExists StorageErrorKind = "already exists"
	// ErrConflict means a conditional mutation's expected version did not
	// match, including when the record no longer exists.
	ErrConflict StorageErrorKind = "version conflict"
	// ErrInvalidArgument means an input violates the contract or backend limits.
	ErrInvalidArgument StorageErrorKind = "invalid argument"
	// ErrUnauthenticated means credentials are missing, invalid or expired.
	ErrUnauthenticated StorageErrorKind = "unauthenticated"
	// ErrPermissionDenied means the caller is not authorized for the operation.
	ErrPermissionDenied StorageErrorKind = "permission denied"
	// ErrThrottled means the backend rejected the request due to a rate limit.
	ErrThrottled StorageErrorKind = "throttled"
	// ErrResourceExhausted means a capacity or quota limit was reached.
	ErrResourceExhausted StorageErrorKind = "resource exhausted"
	// ErrUnavailable means the service or transport is currently unavailable.
	ErrUnavailable StorageErrorKind = "unavailable"
	// ErrCanceled means the operation was canceled.
	ErrCanceled StorageErrorKind = "canceled"
	// ErrDeadlineExceeded means the operation ran out of time.
	ErrDeadlineExceeded StorageErrorKind = "deadline exceeded"
	// ErrUnsupported means the requested capability or configuration is unsupported.
	ErrUnsupported StorageErrorKind = "unsupported"
	// ErrOutcomeUnknown means a mutation may have committed but its outcome
	// cannot be established. Reconcile before deciding whether to repeat it.
	ErrOutcomeUnknown StorageErrorKind = "mutation outcome unknown"
)

// Error implements error so kinds can be used directly with errors.Is.
func (kind StorageErrorKind) Error() string {
	if kind == "" {
		kind = ErrUnknown
	}
	return "storage: " + string(kind)
}

// StorageError is a standardized failure with optional provider context.
// Use errors.Is(err, ErrConflict) for classification and errors.As to inspect
// this type or the wrapped SDK error. A nil Cause is valid for local validation.
// Classification alone never guarantees that retrying a mutation is safe.
type StorageError struct {
	Kind StorageErrorKind
	// Operation is a stable operation name, e.g. "kv.create" or "blob.open".
	Operation string
	// Provider identifies the adapter, e.g. "aws". Empty means unspecified.
	Provider string
	// Cause preserves the original provider or context error for unwrapping.
	// Use errors.Join when both an SDK error and a context error must survive.
	Cause error
	// OutcomeUnknown adds ErrOutcomeUnknown classification without losing the
	// primary Kind, e.g. ErrDeadlineExceeded. Set only for mutations that may
	// have committed. False alone does not guarantee retry safety.
	OutcomeUnknown bool
}

var _ error = (*StorageError)(nil)

// Error reports the classification and operation context. It deliberately does
// not render Cause, which may contain keys, URLs or credentials. Provider and
// Operation must be static labels, never caller data. Inspect Cause explicitly
// when provider diagnostics are needed.
func (e *StorageError) Error() string {
	if e == nil {
		return ErrUnknown.Error()
	}
	message := e.Kind.Error()
	if e.OutcomeUnknown && e.Kind != ErrOutcomeUnknown {
		message += " (mutation outcome unknown)"
	}
	if e.Provider != "" {
		message += " [" + e.Provider + "]"
	}
	if e.Operation != "" {
		message += " during " + e.Operation
	}
	return message
}

// Unwrap exposes the original error, preserving errors.Is and errors.As.
func (e *StorageError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Is matches the standardized kind or uncertain mutation outcome. The standard
// errors package separately traverses Cause; Is does not inspect error strings.
func (e *StorageError) Is(target error) bool {
	if e == nil {
		return false
	}
	kind, ok := target.(StorageErrorKind)
	if !ok {
		return false
	}
	actual := e.Kind
	if actual == "" {
		actual = ErrUnknown
	}
	return actual == kind || (kind == ErrOutcomeUnknown && e.OutcomeUnknown)
}
