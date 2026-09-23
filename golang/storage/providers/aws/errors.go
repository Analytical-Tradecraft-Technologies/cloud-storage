package awsprovider

import (
	"context"
	"errors"
	"net"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func failure(op string, kind contracts.StorageErrorKind, cause error) *contracts.StorageError {
	return &contracts.StorageError{Kind: kind, Operation: op, Provider: "aws", Cause: cause}
}

// A received rejection can be definitive; transport failures and server failures
// after submission remain ambiguous. Mutation retries are disabled in the SDK.
func wrapError(ctx context.Context, op string, cause error, mutation bool) error {
	kind := contracts.ErrUnknown
	definitive := false
	var api smithy.APIError
	if errors.As(cause, &api) {
		switch api.ErrorCode() {
		case "ResourceNotFoundException", "NoSuchKey", "NoSuchBucket", "NotFound":
			kind = contracts.ErrNotFound
			definitive = true
		case "ConditionalCheckFailedException", "PreconditionFailed", "ConditionalRequestConflict":
			kind = contracts.ErrConflict
			definitive = true
		case "AccessDenied", "AccessDeniedException", "Forbidden":
			kind = contracts.ErrPermissionDenied
			definitive = true
		case "InvalidAccessKeyId", "InvalidClientTokenId", "UnrecognizedClientException", "ExpiredToken", "ExpiredTokenException", "SignatureDoesNotMatch":
			kind = contracts.ErrUnauthenticated
			definitive = true
		case "ProvisionedThroughputExceededException", "ThrottlingException", "RequestLimitExceeded", "SlowDown", "Throttling":
			kind = contracts.ErrThrottled
			definitive = true
		case "LimitExceededException", "ItemCollectionSizeLimitExceededException":
			kind = contracts.ErrResourceExhausted
			definitive = true
		case "ValidationException", "InvalidArgument", "InvalidRequest", "EntityTooLarge", "KeyTooLongError":
			kind = contracts.ErrInvalidArgument
			definitive = true
		case "NotImplemented", "UnsupportedOperation":
			kind = contracts.ErrUnsupported
			definitive = true
		case "InternalServerError", "InternalError", "ServiceUnavailable":
			kind = contracts.ErrUnavailable
		}
	}
	var response *smithyhttp.ResponseError
	if errors.As(cause, &response) {
		switch response.HTTPStatusCode() {
		case 401:
			kind = contracts.ErrUnauthenticated
			definitive = true
		case 403:
			kind = contracts.ErrPermissionDenied
			definitive = true
		case 404:
			kind = contracts.ErrNotFound
			definitive = true
		case 412:
			kind = contracts.ErrConflict
			definitive = true
		case 429:
			kind = contracts.ErrThrottled
			definitive = true
		}
		if response.HTTPStatusCode() >= 500 {
			kind = contracts.ErrUnavailable
			definitive = false
		}
	}
	var network net.Error
	if errors.As(cause, &network) && kind == contracts.ErrUnknown {
		kind = contracts.ErrUnavailable
	}
	// Preserve context identity, even when the SDK returns a different wrapper.
	if !definitive && ctx.Err() != nil && !errors.Is(cause, ctx.Err()) {
		cause = errors.Join(cause, ctx.Err())
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		kind = contracts.ErrDeadlineExceeded
	}
	if errors.Is(cause, context.Canceled) {
		kind = contracts.ErrCanceled
	}
	result := failure(op, kind, cause)
	result.OutcomeUnknown = mutation && !definitive
	return result
}
