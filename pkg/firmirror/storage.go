package firmirror

import (
	"context"
	"io"
)

// Interface for different storage backends
type Storage interface {
	// Write stores data with the given key
	Write(ctx context.Context, key string, data io.Reader) error

	// Read retrieves data for the given key
	Read(ctx context.Context, key string) (io.ReadCloser, error)

	// Exists checks if a key exists
	Exists(ctx context.Context, key string) (bool, error)
}

