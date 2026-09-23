package awsprovider

import (
	"context"
	"errors"
	"io"
	"os"
	"unicode/utf8"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// MaximumBlobBytes bounds the initial single-PutObject implementation. Larger
// objects need a future multipart implementation with conditional completion.
const MaximumBlobBytes int64 = 5_000_000_000

type s3Store struct {
	client        s3API
	bucket        string
	tempDirectory string
}

var _ blob.BlobStore = (*s3Store)(nil)

func validateBlobKey(key blob.BlobKey) error {
	if key == "" || !utf8.ValidString(string(key)) || len(key) > 1024 {
		return failure("blob.key", contracts.ErrInvalidArgument, nil)
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func (s *s3Store) Create(ctx context.Context, key blob.BlobKey, body io.Reader, size int64) error {
	const op = "blob.create"
	if err := validateBlobKey(key); err != nil {
		return err
	}
	if body == nil || size < 0 || size > MaximumBlobBytes {
		return failure(op, contracts.ErrInvalidArgument, nil)
	}
	if err := ctx.Err(); err != nil {
		return wrapError(ctx, op, err, false)
	}
	// Validate size AND EOF before publication; streaming straight to PutObject
	// could publish the first size bytes before discovering an oversized input.
	file, err := os.CreateTemp(s.tempDirectory, "cloud-storage-blob-*")
	if err != nil {
		return failure(op, contracts.ErrUnknown, err)
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	n, err := io.Copy(file, io.LimitReader(contextReader{ctx: ctx, reader: body}, size+1))
	if err != nil {
		return wrapError(ctx, op, err, false)
	}
	if n != size {
		return failure(op, contracts.ErrInvalidArgument, nil)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return failure(op, contracts.ErrUnknown, err)
	}
	if err := ctx.Err(); err != nil {
		return wrapError(ctx, op, err, false)
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(string(key)), Body: file, ContentLength: aws.Int64(size), IfNoneMatch: aws.String("*")})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && api.ErrorCode() == "PreconditionFailed" {
			return failure(op, contracts.ErrAlreadyExists, err)
		}
		return wrapError(ctx, op, err, true)
	}
	return nil
}

func (s *s3Store) Open(ctx context.Context, key blob.BlobKey) (blob.BlobReadResult, error) {
	const op = "blob.open"
	if err := validateBlobKey(key); err != nil {
		return blob.BlobReadResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return blob.BlobReadResult{}, wrapError(ctx, op, err, false)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(string(key))})
	if err != nil {
		if out != nil && out.Body != nil {
			_ = out.Body.Close()
		}
		return blob.BlobReadResult{}, wrapError(ctx, op, err, false)
	}
	if out == nil || out.Body == nil || out.ContentLength == nil || *out.ContentLength < 0 {
		if out != nil && out.Body != nil {
			_ = out.Body.Close()
		}
		return blob.BlobReadResult{}, failure(op, contracts.ErrUnknown, nil)
	}
	return blob.BlobReadResult{Body: &blobBody{ctx: ctx, body: out.Body}, Size: *out.ContentLength}, nil
}

// Preserve standardized errors for failures that happen after Open returns.
type blobBody struct {
	ctx  context.Context
	body io.ReadCloser
}

func (b *blobBody) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, wrapError(b.ctx, "blob.read", err, false)
	}
	n, err := b.body.Read(p)
	if err != nil && err != io.EOF {
		return n, wrapError(b.ctx, "blob.read", err, false)
	}
	return n, err
}
func (b *blobBody) Close() error {
	if err := b.body.Close(); err != nil {
		return wrapError(b.ctx, "blob.close", err, false)
	}
	return nil
}

func (s *s3Store) Delete(ctx context.Context, key blob.BlobKey) error {
	const op = "blob.delete"
	if err := validateBlobKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return wrapError(ctx, op, err, false)
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(string(key))})
	if err != nil {
		var api smithy.APIError
		// Missing bucket is NOT a missing object: only NoSuchKey is a no-op.
		if errors.As(err, &api) && api.ErrorCode() == "NoSuchKey" {
			return nil
		}
		return wrapError(ctx, op, err, true)
	}
	return nil
}
