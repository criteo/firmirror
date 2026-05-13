package hpe

import (
	"path/filepath"
	"testing"

	"github.com/premday/firmirror/pkg/lvfs"
)

func TestGoldenAppStream(t *testing.T) {
	tmpDir := t.TempDir()
	mockFirmwarePath := createMockHPEFirmware(t, tmpDir)

	entry := &HPEFirmwareEntry{
		Filename:     "test-firmware.fwpkg",
		Entry:        &HPECatalogEntry{},
		DownloadPath: mockFirmwarePath,
	}

	components, err := entry.ToAppstream()
	if err != nil {
		t.Fatalf("ToAppstream failed: %v", err)
	}

	goldenPath := filepath.Join("testdata", "golden.xml")
	lvfs.AssertGoldenComponents(t, goldenPath, components)
}
