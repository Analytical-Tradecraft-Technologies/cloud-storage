package awsprovider

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type fakeS3 struct {
	s3API
	put    func(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	get    func(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	delete func(*s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
	head   func(*s3.HeadBucketInput) (*s3.HeadBucketOutput, error)
	list   func(*s3.ListBucketsInput) (*s3.ListBucketsOutput, error)
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return f.put(in)
}
func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return f.get(in)
}
func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return f.delete(in)
}
func (f *fakeS3) HeadBucket(_ context.Context, in *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return f.head(in)
}
func (f *fakeS3) ListBuckets(_ context.Context, in *s3.ListBucketsInput, _ ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	return f.list(in)
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

func TestS3StagesExactLengthAndCleansUp(t *testing.T) {
	for _, data := range []string{"", "hello"} {
		t.Run(data, func(t *testing.T) {
			directory := t.TempDir()
			calls := 0
			f := &fakeS3{put: func(in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
				calls++
				if aws.ToString(in.IfNoneMatch) != "*" || aws.ToString(in.Bucket) != "bucket-a" || aws.ToInt64(in.ContentLength) != int64(len(data)) {
					t.Fatal("wrong publication parameters")
				}
				staged, ok := in.Body.(*os.File)
				if !ok {
					t.Fatal("not staged")
				}
				info, err := staged.Stat()
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode().Perm() != 0600 {
					t.Fatal("insecure staging permissions")
				}
				b, err := io.ReadAll(in.Body)
				if err != nil || string(b) != data {
					t.Fatal("bad body")
				}
				return &s3.PutObjectOutput{}, nil
			}}
			store := &s3Store{client: f, bucket: "bucket-a", tempDirectory: directory}
			input := &trackedBody{Reader: strings.NewReader(data)}
			if err := store.Create(context.Background(), "key", input, int64(len(data))); err != nil {
				t.Fatal(err)
			}
			if input.closed {
				t.Fatal("closed caller input")
			}
			if calls != 1 {
				t.Fatal("wrong request count")
			}
			files, err := os.ReadDir(directory)
			if err != nil || len(files) != 0 {
				t.Fatal("staging leak")
			}
		})
	}
}

func TestS3InvalidBodyNeverPublishes(t *testing.T) {
	directory := t.TempDir()
	store := &s3Store{client: &fakeS3{}, bucket: "bucket", tempDirectory: directory}
	for _, test := range []struct {
		data string
		size int64
	}{{"short", 6}, {"long", 3}, {"unexpected", 0}, {"", -1}, {"", MaximumBlobBytes + 1}} {
		if err := store.Create(context.Background(), "key", strings.NewReader(test.data), test.size); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatal(err)
		}
	}
	if err := store.Create(context.Background(), "key", nil, 0); !errors.Is(err, contracts.ErrInvalidArgument) {
		t.Fatal(err)
	}
	files, err := os.ReadDir(directory)
	if err != nil || len(files) != 0 {
		t.Fatal("staging leak")
	}
}

func TestS3CreateFailurePreservesCauseAndCleansUp(t *testing.T) {
	for _, code := range []string{"PreconditionFailed", "ConditionalRequestConflict", "InternalError"} {
		t.Run(code, func(t *testing.T) {
			directory := t.TempDir()
			cause := &smithy.GenericAPIError{Code: code, Message: "private-object-key"}
			f := &fakeS3{put: func(*s3.PutObjectInput) (*s3.PutObjectOutput, error) { return nil, cause }}
			err := (&s3Store{client: f, bucket: "bucket", tempDirectory: directory}).Create(context.Background(), "key", strings.NewReader("x"), 1)
			if !errors.Is(err, cause) {
				t.Fatal("cause lost")
			}
			var sdk *smithy.GenericAPIError
			if !errors.As(err, &sdk) {
				t.Fatal("SDK type lost")
			}
			if strings.Contains(err.Error(), "private-object-key") {
				t.Fatal("error leaks key")
			}
			if code == "PreconditionFailed" && !errors.Is(err, contracts.ErrAlreadyExists) {
				t.Fatal(err)
			}
			if code == "ConditionalRequestConflict" && !errors.Is(err, contracts.ErrConflict) {
				t.Fatal(err)
			}
			if errors.Is(err, contracts.ErrOutcomeUnknown) != (code == "InternalError") {
				t.Fatal("wrong uncertainty")
			}
			files, _ := os.ReadDir(directory)
			if len(files) != 0 {
				t.Fatal("staging leak")
			}
		})
	}
}

type failedReader struct{ err error }

func (r failedReader) Read([]byte) (int, error) { return 0, r.err }
func TestS3StreamingErrorsAndCancellation(t *testing.T) {
	cause := errors.New("stream failed")
	body := &trackedBody{Reader: failedReader{cause}}
	f := &fakeS3{get: func(in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{Body: body, ContentLength: aws.Int64(12)}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, err := (&s3Store{client: f, bucket: "bucket"}).Open(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	_, err = result.Body.Read(make([]byte, 1))
	var detail *contracts.StorageError
	if !errors.As(err, &detail) || !errors.Is(err, cause) {
		t.Fatal("stream error not wrapped")
	}
	cancel()
	_, err = result.Body.Read(make([]byte, 1))
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := result.Body.Close(); err != nil || !body.closed {
		t.Fatal("body not closed")
	}
}

func TestS3DeleteMissingObjectButNotBucket(t *testing.T) {
	for _, code := range []string{"", "NoSuchKey", "NoSuchBucket", "AccessDenied"} {
		f := &fakeS3{delete: func(in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
			if aws.ToString(in.Bucket) != "bucket" || aws.ToString(in.Key) != "key" {
				t.Fatal("wrong target")
			}
			if code == "" {
				return &s3.DeleteObjectOutput{}, nil
			}
			return nil, &smithy.GenericAPIError{Code: code}
		}}
		err := (&s3Store{client: f, bucket: "bucket"}).Delete(context.Background(), "key")
		if (err == nil) != (code == "" || code == "NoSuchKey") {
			t.Fatalf("%s: %v", code, err)
		}
	}
}
