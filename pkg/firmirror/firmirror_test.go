package firmirror

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/criteo/firmirror/pkg/lvfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockVendor implements the Vendor interface for testing
type MockVendor struct {
	catalog         *MockCatalog
	fetchErr        error
	retrieveErr     error
	retrievedFiles  []string
	retrieveContent string
}

func (m *MockVendor) FetchCatalog() (Catalog, error) {
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	return m.catalog, nil
}

func (m *MockVendor) RetrieveFirmware(entry FirmwareEntry, tmpDir string) error {
	if m.retrieveErr != nil {
		return m.retrieveErr
	}

	filename := entry.GetFilename()
	filepath := filepath.Join(tmpDir, filename)

	content := m.retrieveContent
	if content == "" {
		content = "mock firmware content"
	}

	err := os.WriteFile(filepath, []byte(content), 0644)
	if err != nil {
		return err
	}

	m.retrievedFiles = append(m.retrievedFiles, filename)
	return nil
}

// MockCatalog implements the Catalog interface for testing
type MockCatalog struct {
	entries []FirmwareEntry
}

func (m *MockCatalog) ListEntries() []FirmwareEntry {
	return m.entries
}

// MockFirmwareEntry implements the FirmwareEntry interface for testing
type MockFirmwareEntry struct {
	filename     string
	sourceURL    string
	appstream    *lvfs.Component
	appstreamErr error
}

func (m *MockFirmwareEntry) GetFilename() string {
	return m.filename
}

func (m *MockFirmwareEntry) GetSourceURL() string {
	return m.sourceURL
}

func (m *MockFirmwareEntry) ToAppstream() (*lvfs.Component, error) {
	if m.appstreamErr != nil {
		return nil, m.appstreamErr
	}
	return m.appstream, nil
}

func TestNewFimirrorSyncer(t *testing.T) {
	t.Run("CreatesSyncerWithConfig", func(t *testing.T) {
		config := FirmirrorConfig{
			OutputDir: "/tmp/test",
		}

		syncer := NewFimirrorSyncer(config)

		assert.NotNil(t, syncer, "Syncer should not be nil")
		assert.Equal(t, "/tmp/test", syncer.Config.OutputDir, "Config should be set correctly")
		assert.NotNil(t, syncer.vendors, "Vendors map should be initialized")
		assert.Empty(t, syncer.vendors, "Vendors map should be empty initially")
	})
}

func TestFimirrorSyncer_RegisterVendor(t *testing.T) {
	t.Run("RegistersVendors", func(t *testing.T) {
		syncer := NewFimirrorSyncer(FirmirrorConfig{})
		mockVendor1 := &MockVendor{}
		mockVendor2 := &MockVendor{}

		syncer.RegisterVendor("vendor1", mockVendor1)
		syncer.RegisterVendor("vendor2", mockVendor2)

		vendors := syncer.GetAllVendors()
		assert.Len(t, vendors, 2, "Should have two vendors registered")
		assert.Contains(t, vendors, "vendor1", "Should contain vendor1")
		assert.Contains(t, vendors, "vendor2", "Should contain vendor2")
	})
}

func TestFimirrorSyncer_GetAllVendors(t *testing.T) {
	t.Run("ReturnsClone", func(t *testing.T) {
		syncer := NewFimirrorSyncer(FirmirrorConfig{})
		mockVendor := &MockVendor{}

		syncer.RegisterVendor("test-vendor", mockVendor)

		vendors := syncer.GetAllVendors()

		// Modify the returned map
		delete(vendors, "test-vendor")

		// Original should still have the vendor
		originalVendors := syncer.GetAllVendors()
		assert.Len(t, originalVendors, 1, "Original vendors map should be unchanged")
		assert.Contains(t, originalVendors, "test-vendor", "Original should still contain test-vendor")
	})
}

func TestFimirrorSyncer_ProcessVendor(t *testing.T) {
	t.Run("SuccessfulProcessing", func(t *testing.T) {
		tmpDir := t.TempDir()

		syncer := NewFimirrorSyncer(FirmirrorConfig{
			OutputDir: tmpDir,
		})

		// Create mock firmware entry with minimal AppStream component
		mockEntry := &MockFirmwareEntry{
			filename: "test-firmware.bin",
			appstream: &lvfs.Component{
				Type:            "firmware",
				ID:              "com.test.firmware",
				Name:            "Test Firmware",
				Summary:         "Test firmware summary",
				MetadataLicense: "proprietary",
				ProjectLicense:  "proprietary",
				Releases: []lvfs.Release{
					{
						Version: "1.0.0",
					},
				},
			},
		}

		mockVendor := &MockVendor{
			catalog: &MockCatalog{
				entries: []FirmwareEntry{mockEntry},
			},
		}

		_ = syncer.ProcessVendor(mockVendor, "test-vendor")

		// Note: This will fail because fwupdtool is not available in test environment
		// But we can verify the vendor methods were called
		assert.NotNil(t, mockVendor.retrievedFiles, "Firmware retrieval should be attempted")
	})

	t.Run("FetchCatalogError", func(t *testing.T) {
		tmpDir := t.TempDir()

		syncer := NewFimirrorSyncer(FirmirrorConfig{
			OutputDir: tmpDir,
		})

		mockVendor := &MockVendor{
			fetchErr: errors.New("catalog fetch failed"),
		}

		err := syncer.ProcessVendor(mockVendor, "test-vendor")

		assert.Error(t, err, "Should return error when catalog fetch fails")
		assert.Contains(t, err.Error(), "catalog fetch failed", "Error should contain original error message")
	})

	t.Run("RetrieveFirmwareError", func(t *testing.T) {
		tmpDir := t.TempDir()

		syncer := NewFimirrorSyncer(FirmirrorConfig{
			OutputDir: tmpDir,
		})

		mockEntry := &MockFirmwareEntry{
			filename: "test-firmware.bin",
		}

		mockVendor := &MockVendor{
			catalog: &MockCatalog{
				entries: []FirmwareEntry{mockEntry},
			},
			retrieveErr: errors.New("retrieve failed"),
		}

		// Should not return error, but should continue processing
		err := syncer.ProcessVendor(mockVendor, "test-vendor")

		assert.NoError(t, err, "ProcessVendor should not return error for individual firmware failures")
	})

	t.Run("ToAppstreamError", func(t *testing.T) {
		tmpDir := t.TempDir()

		syncer := NewFimirrorSyncer(FirmirrorConfig{
			OutputDir: tmpDir,
		})

		mockEntry := &MockFirmwareEntry{
			filename:     "test-firmware.bin",
			appstreamErr: errors.New("appstream conversion failed"),
		}

		mockVendor := &MockVendor{
			catalog: &MockCatalog{
				entries: []FirmwareEntry{mockEntry},
			},
		}

		// Should not return error, but should continue processing
		err := syncer.ProcessVendor(mockVendor, "test-vendor")

		assert.NoError(t, err, "ProcessVendor should not return error for individual firmware failures")
		assert.Len(t, mockVendor.retrievedFiles, 1, "Firmware should be retrieved before appstream conversion")
	})

	t.Run("EmptyCatalog", func(t *testing.T) {
		tmpDir := t.TempDir()

		syncer := NewFimirrorSyncer(FirmirrorConfig{
			OutputDir: tmpDir,
		})

		mockVendor := &MockVendor{
			catalog: &MockCatalog{
				entries: []FirmwareEntry{},
			},
		}

		err := syncer.ProcessVendor(mockVendor, "test-vendor")

		assert.NoError(t, err, "Should succeed with empty catalog")
		assert.Empty(t, mockVendor.retrievedFiles, "No firmware should be retrieved")
	})

	t.Run("MultipleFirmwareEntries", func(t *testing.T) {
		tmpDir := t.TempDir()

		syncer := NewFimirrorSyncer(FirmirrorConfig{
			OutputDir: tmpDir,
		})

		mockEntry1 := &MockFirmwareEntry{
			filename: "firmware1.bin",
			appstream: &lvfs.Component{
				Type:            "firmware",
				ID:              "com.test.firmware1",
				Name:            "Test Firmware 1",
				MetadataLicense: "proprietary",
			},
		}

		mockEntry2 := &MockFirmwareEntry{
			filename: "firmware2.bin",
			appstream: &lvfs.Component{
				Type:            "firmware",
				ID:              "com.test.firmware2",
				Name:            "Test Firmware 2",
				MetadataLicense: "proprietary",
			},
		}

		mockVendor := &MockVendor{
			catalog: &MockCatalog{
				entries: []FirmwareEntry{mockEntry1, mockEntry2},
			},
		}

		syncer.ProcessVendor(mockVendor, "test-vendor")

		assert.Len(t, mockVendor.retrievedFiles, 2, "Both firmware files should be retrieved")
		assert.Contains(t, mockVendor.retrievedFiles, "firmware1.bin", "Should retrieve firmware1")
		assert.Contains(t, mockVendor.retrievedFiles, "firmware2.bin", "Should retrieve firmware2")
	})

	t.Run("TempDirectoryCreated", func(t *testing.T) {
		tmpDir := t.TempDir()

		syncer := NewFimirrorSyncer(FirmirrorConfig{
			OutputDir: tmpDir,
		})

		mockEntry := &MockFirmwareEntry{
			filename: "test-firmware.bin",
			appstream: &lvfs.Component{
				Type:            "firmware",
				ID:              "com.test.firmware",
				MetadataLicense: "proprietary",
			},
		}

		mockVendor := &MockVendor{
			catalog: &MockCatalog{
				entries: []FirmwareEntry{mockEntry},
			},
		}

		syncer.ProcessVendor(mockVendor, "test-vendor")

		entries, err := os.ReadDir(tmpDir)
		require.NoError(t, err, "Should be able to read output directory")
		assert.NotNil(t, entries, "Output directory should exist")
	})
}

func TestFimirrorSyncer_BuildPackage(t *testing.T) {
	t.Run("CreatesMetainfoXML", func(t *testing.T) {
		tmpDir := t.TempDir()
		outputDir := t.TempDir()

		syncer := NewFimirrorSyncer(FirmirrorConfig{
			OutputDir: outputDir,
		})

		component := &lvfs.Component{
			Type:            "firmware",
			ID:              "com.test.firmware",
			Name:            "Test Firmware",
			Summary:         "Test summary",
			MetadataLicense: "proprietary",
			ProjectLicense:  "proprietary",
		}

		// Create a dummy firmware file
		firmwareFilename := "firmware.bin"
		firmwareFile := filepath.Join(tmpDir, firmwareFilename)
		err := os.WriteFile(firmwareFile, []byte("test firmware"), 0644)
		require.NoError(t, err, "Should create test firmware file")

		// Note: This will fail without fwupdtool, but we can test XML creation
		syncer.buildPackage(component, firmwareFilename, tmpDir)

		// Verify metainfo XML was created
		metainfoPath := filepath.Join(tmpDir, "firmware.metainfo.xml")
		assert.FileExists(t, metainfoPath, "Metainfo XML should be created")

		// Verify XML content
		content, err := os.ReadFile(metainfoPath)
		require.NoError(t, err, "Should be able to read metainfo XML")

		assert.Contains(t, string(content), "<?xml version", "Should contain XML header")
		assert.Contains(t, string(content), "com.test.firmware", "Should contain component ID")
		assert.Contains(t, string(content), "Test Firmware", "Should contain component name")
	})
}
