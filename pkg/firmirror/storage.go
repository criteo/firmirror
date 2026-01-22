package firmirror

import "io"

// Interface for different storage backends
type Storage interface {
	// Write stores data with the given key
	Write(key string, data io.Reader) error

	// Read retrieves data for the given key
	Read(key string) (io.ReadCloser, error)

	// Exists checks if a key exists
	Exists(key string) (bool, error)
}

