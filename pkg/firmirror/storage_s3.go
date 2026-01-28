package firmirror

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Storage implements Storage interface for AWS S3 or S3-compatible storage
type S3Storage struct {
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	bucket     string
	prefix     string // optional prefix for all keys
}

func NewS3Storage(ctx context.Context, bucket, prefix, region, endpoint string) (*S3Storage, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	var opts []func(*config.LoadOptions) error

	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		}
	})

	// Verify bucket exists
	if _, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return nil, fmt.Errorf("failed to access bucket %s: %w", bucket, err)
	}

	return &S3Storage{
		client:     client,
		uploader:   manager.NewUploader(client),
		downloader: manager.NewDownloader(client),
		bucket:     bucket,
		prefix:     prefix,
	}, nil
}

// buildKey constructs the full S3 key with optional prefix
func (s *S3Storage) buildKey(key string) string {
	if s.prefix != "" {
		return s.prefix + "/" + key
	}
	return key
}

// Write stores data with the given key to S3
func (s *S3Storage) Write(ctx context.Context, key string, data io.Reader) error {
	fullKey := s.buildKey(key)

	// Read data into buffer to determine size (needed for some S3-compatible services)
	// This also allows us to retry in case of transient errors
	buf, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}

	_, err = s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
		Body:   bytes.NewReader(buf),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

// Read retrieves data for the given key from S3
func (s *S3Storage) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	fullKey := s.buildKey(key)

	// Download to buffer
	buf := manager.NewWriteAtBuffer([]byte{})
	_, err := s.downloader.Download(ctx, buf, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download from S3: %w", err)
	}

	// Return buffer as ReadCloser
	return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
}

// Exists checks if a key exists in S3
func (s *S3Storage) Exists(ctx context.Context, key string) (bool, error) {
	fullKey := s.buildKey(key)

	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		// Check if it's a not found error
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check object existence: %w", err)
	}

	return true, nil
}

// List returns all keys with the given prefix (useful for debugging and management)
func (s *S3Storage) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := s.buildKey(prefix)

	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key != nil {
				// Remove the storage prefix from the returned keys
				key := *obj.Key
				if s.prefix != "" && len(key) > len(s.prefix)+1 {
					key = key[len(s.prefix)+1:]
				}
				keys = append(keys, key)
			}
		}
	}

	return keys, nil
}
