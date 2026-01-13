package utils

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func ReaderToFile(src io.Reader, dst string) error {
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	buffer := make([]byte, 4096)

	_, err = io.CopyBuffer(out, src, buffer)
	if err != nil {
		return err
	}

	return nil
}

func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	err = ReaderToFile(in, dst)
	if err != nil {
		return err
	}

	return nil
}

func DownloadFile(url string) (io.ReadCloser, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return resp.Body, nil
}
