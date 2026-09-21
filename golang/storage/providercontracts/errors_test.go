package providercontracts_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

type sdkError struct{ requestID string }

func (e *sdkError) Error() string { return "provider diagnostic: secret-key" }

func TestStorageErrorPreservesClassificationAndCause(t *testing.T) {
	provider := &sdkError{requestID: "request-123"}
	original := &providercontracts.StorageError{
		Kind:     providercontracts.ErrDeadlineExceeded,
		Provider: "aws", Operation: "kv.replace",
		Cause:          errors.Join(provider, context.DeadlineExceeded),
		OutcomeUnknown: true,
	}
	err := fmt.Errorf("application: %w", original)
	for _, target := range []error{providercontracts.ErrDeadlineExceeded, providercontracts.ErrOutcomeUnknown, context.DeadlineExceeded, provider} {
		if !errors.Is(err, target) {
			t.Fatalf("missing classification or cause: %T", target)
		}
	}
	if errors.Is(err, providercontracts.ErrConflict) {
		t.Fatal("spurious conflict classification")
	}
	var detail *providercontracts.StorageError
	if !errors.As(err, &detail) || detail != original {
		t.Fatal("structured error lost")
	}
	var sdk *sdkError
	if !errors.As(err, &sdk) || sdk != provider {
		t.Fatal("SDK error lost")
	}
	if strings.Contains(err.Error(), "secret-key") {
		t.Fatal("cause leaked into default message")
	}
	if !strings.Contains(err.Error(), "kv.replace") || !strings.Contains(err.Error(), "mutation outcome unknown") {
		t.Fatal("missing diagnostic context")
	}
}

func TestStorageErrorKinds(t *testing.T) {
	kinds := []providercontracts.StorageErrorKind{
		providercontracts.ErrUnknown, providercontracts.ErrNotFound, providercontracts.ErrAlreadyExists,
		providercontracts.ErrConflict, providercontracts.ErrInvalidArgument, providercontracts.ErrUnauthenticated,
		providercontracts.ErrPermissionDenied, providercontracts.ErrThrottled, providercontracts.ErrResourceExhausted,
		providercontracts.ErrUnavailable, providercontracts.ErrCanceled, providercontracts.ErrDeadlineExceeded,
		providercontracts.ErrUnsupported, providercontracts.ErrOutcomeUnknown,
	}
	for _, kind := range kinds {
		err := &providercontracts.StorageError{Kind: kind}
		if !errors.Is(err, kind) {
			t.Fatalf("kind %s not recognized", kind)
		}
		if errors.Unwrap(err) != nil {
			t.Fatal("unexpected cause")
		}
		for _, other := range kinds {
			if other != kind && errors.Is(err, other) {
				t.Fatalf("%s matches %s", kind, other)
			}
		}
	}
	if !errors.Is(&providercontracts.StorageError{}, providercontracts.ErrUnknown) {
		t.Fatal("zero kind must be unknown")
	}
	var nilError *providercontracts.StorageError
	if nilError.Unwrap() != nil || nilError.Is(providercontracts.ErrUnknown) {
		t.Fatal("invalid nil receiver behavior")
	}
	_ = nilError.Error()
}

func TestLocalCodecErrorIsStructured(t *testing.T) {
	var doc kv.KeyValueDocument
	err := doc.UnmarshalBinary([]byte("invalid"))
	var detail *providercontracts.StorageError
	if !errors.Is(err, providercontracts.ErrInvalidArgument) || !errors.As(err, &detail) {
		t.Fatal("codec did not return a structured validation failure")
	}
	if detail.Cause != nil {
		t.Fatal("local validation unexpectedly has a provider cause")
	}
}
