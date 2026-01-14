package utils

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

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

func DownloadFileToDest(url, file string) error {
	out, err := os.Create(file)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := DownloadFile(url)
	if err != nil {
		return err
	}
	defer resp.Close()

	_, err = io.Copy(out, resp)
	return err
}
