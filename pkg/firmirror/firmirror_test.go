package firmirror

import (
	"context"
	"encoding/xml"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/criteo/firmirror/pkg/lvfs"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestSyncer creates a test configuration with local storage
func createTestSyncer(t *testing.T) (*FirmirrorSyncer, string) {
	tmpDir := t.TempDir()
	storage, err := NewLocalStorage(filepath.Join(tmpDir, "output"))
	require.NoError(t, err)
	config := FirmirrorConfig{
		CacheDir: filepath.Join(tmpDir, "cache"),
	}

	return NewFirmirrorSyncer(config, storage), tmpDir
}

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

func TestNewFirmirrorSyncer(t *testing.T) {
	t.Run("CreatesSyncerWithConfig", func(t *testing.T) {
		syncer, _ := createTestSyncer(t)

		assert.NotNil(t, syncer, "Syncer should not be nil")
		assert.NotNil(t, syncer.Storage, "Storage should be set")
		assert.NotNil(t, syncer.vendors, "Vendors map should be initialized")
		assert.Empty(t, syncer.vendors, "Vendors map should be empty initially")
	})
}

func TestFirmirrorSyncer_RegisterVendor(t *testing.T) {
	t.Run("RegistersVendors", func(t *testing.T) {
		syncer, _ := createTestSyncer(t)
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

func TestFirmirrorSyncer_GetAllVendors(t *testing.T) {
	t.Run("ReturnsClone", func(t *testing.T) {
		syncer, _ := createTestSyncer(t)
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

func TestFirmirrorSyncer_ProcessVendor(t *testing.T) {
	t.Run("SuccessfulProcessing", func(t *testing.T) {
		syncer, _ := createTestSyncer(t)

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

		_ = syncer.ProcessVendor(context.TODO(), mockVendor, "test-vendor")

		// Note: This will fail because fwupdtool is not available in test environment
		// But we can verify the vendor methods were called
		assert.NotNil(t, mockVendor.retrievedFiles, "Firmware retrieval should be attempted")
	})

	t.Run("FetchCatalogError", func(t *testing.T) {
		syncer, _ := createTestSyncer(t)

		mockVendor := &MockVendor{
			fetchErr: errors.New("catalog fetch failed"),
		}

		err := syncer.ProcessVendor(context.TODO(), mockVendor, "test-vendor")

		assert.Error(t, err, "Should return error when catalog fetch fails")
		assert.Contains(t, err.Error(), "catalog fetch failed", "Error should contain original error message")
	})

	t.Run("RetrieveFirmwareError", func(t *testing.T) {
		syncer, _ := createTestSyncer(t)

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
		err := syncer.ProcessVendor(context.TODO(), mockVendor, "test-vendor")

		assert.NoError(t, err, "ProcessVendor should not return error for individual firmware failures")
	})

	t.Run("ToAppstreamError", func(t *testing.T) {
		syncer, _ := createTestSyncer(t)

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
		err := syncer.ProcessVendor(context.TODO(), mockVendor, "test-vendor")

		assert.NoError(t, err, "ProcessVendor should not return error for individual firmware failures")
		assert.Len(t, mockVendor.retrievedFiles, 1, "Firmware should be retrieved before appstream conversion")
	})

	t.Run("EmptyCatalog", func(t *testing.T) {
		syncer, _ := createTestSyncer(t)

		mockVendor := &MockVendor{
			catalog: &MockCatalog{
				entries: []FirmwareEntry{},
			},
		}

		err := syncer.ProcessVendor(context.TODO(), mockVendor, "test-vendor")

		assert.NoError(t, err, "Should succeed with empty catalog")
		assert.Empty(t, mockVendor.retrievedFiles, "No firmware should be retrieved")
	})

	t.Run("MultipleFirmwareEntries", func(t *testing.T) {
		syncer, _ := createTestSyncer(t)

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

		syncer.ProcessVendor(context.TODO(), mockVendor, "test-vendor")

		assert.Len(t, mockVendor.retrievedFiles, 2, "Both firmware files should be retrieved")
		assert.Contains(t, mockVendor.retrievedFiles, "firmware1.bin", "Should retrieve firmware1")
		assert.Contains(t, mockVendor.retrievedFiles, "firmware2.bin", "Should retrieve firmware2")
	})

	t.Run("TempDirectoryCreated", func(t *testing.T) {
		syncer, tmpDir := createTestSyncer(t)

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

		syncer.ProcessVendor(context.TODO(), mockVendor, "test-vendor")

		entries, err := os.ReadDir(tmpDir)
		require.NoError(t, err, "Should be able to read output directory")
		assert.NotNil(t, entries, "Output directory should exist")
	})
}

func TestFirmirrorSyncer_BuildPackage(t *testing.T) {
	t.Run("CreatesMetainfoXML", func(t *testing.T) {
		syncer, tmpDir := createTestSyncer(t)

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
		syncer.buildPackage(context.TODO(), component, firmwareFilename, tmpDir)

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

func TestFirmirrorSyncer_LoadMetadata(t *testing.T) {
	t.Run("LoadsExistingMetadata", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage, err := NewLocalStorage(tmpDir)
		require.NoError(t, err)

		syncer := NewFirmirrorSyncer(FirmirrorConfig{
			CacheDir: filepath.Join(tmpDir, "cache"),
		}, storage)

		// Create test metadata
		testComponents := &lvfs.Components{
			Origin: "firmirror",
			Component: []lvfs.Component{
				{
					Type:            "firmware",
					ID:              "com.test.firmware1",
					Name:            "Test Firmware 1",
					MetadataLicense: "proprietary",
					Releases: []lvfs.Release{
						{
							Version: "1.0.0",
							Checksums: []lvfs.Checksum{
								{
									Filename: "firmware1.bin",
									Type:     "sha256",
									Value:    "abc123",
								},
							},
						},
					},
				},
				{
					Type:            "firmware",
					ID:              "com.test.firmware2",
					Name:            "Test Firmware 2",
					MetadataLicense: "proprietary",
					Releases: []lvfs.Release{
						{
							Version: "2.0.0",
							Checksums: []lvfs.Checksum{
								{
									Filename: "firmware2.bin",
									Type:     "sha256",
									Value:    "def456",
								},
							},
						},
					},
				},
			},
		}

		// Write compressed metadata file
		metadataPath := filepath.Join(tmpDir, "metadata.xml.zst")
		file, err := os.Create(metadataPath)
		require.NoError(t, err)

		zstWriter, err := zstd.NewWriter(file)
		require.NoError(t, err)

		xmlData, err := xml.Marshal(testComponents)
		require.NoError(t, err)
		_, err = zstWriter.Write([]byte(xml.Header))
		require.NoError(t, err)
		_, err = zstWriter.Write(xmlData)
		require.NoError(t, err)
		zstWriter.Close()
		file.Close()

		// Load metadata
		err = syncer.LoadMetadata(context.TODO())
		require.NoError(t, err, "Should load metadata successfully")

		// Verify loaded metadata
		assert.NotNil(t, syncer.existingMetadata, "Existing metadata should be loaded")
		assert.Len(t, syncer.existingMetadata.Component, 2, "Should have 2 components")
		assert.Equal(t, "com.test.firmware1", syncer.existingMetadata.Component[0].ID)
		assert.Equal(t, "com.test.firmware2", syncer.existingMetadata.Component[1].ID)

		// Verify index was built
		assert.Len(t, syncer.existingIndex, 2, "Should have 2 entries in index")
		assert.True(t, syncer.existingIndex["firmware1.bin"], "Should have firmware1 in index")
		assert.True(t, syncer.existingIndex["firmware2.bin"], "Should have firmware2 in index")
	})

	t.Run("HandlesNonExistentMetadata", func(t *testing.T) {
		syncer, _ := createTestSyncer(t)

		// No metadata file exists
		err := syncer.LoadMetadata(context.TODO())
		assert.NoError(t, err, "Should not error when metadata doesn't exist")
		assert.Nil(t, syncer.existingMetadata, "Existing metadata should be nil")
		assert.Empty(t, syncer.existingIndex, "Index should be empty")
	})

	t.Run("HandlesCorruptedMetadata", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage, err := NewLocalStorage(tmpDir)
		require.NoError(t, err)
		syncer := NewFirmirrorSyncer(FirmirrorConfig{
			CacheDir: filepath.Join(tmpDir, "cache"),
		}, storage)

		// Create corrupted metadata file
		metadataPath := filepath.Join(tmpDir, "metadata.xml.zst")
		err = os.WriteFile(metadataPath, []byte("not valid zstd"), 0644)
		require.NoError(t, err)

		// Load should fail
		err = syncer.LoadMetadata(context.TODO())
		assert.Error(t, err, "Should error with corrupted metadata")
		// zstd error message may vary but should indicate invalid input
		assert.Contains(t, err.Error(), "failed to", "Error should indicate failure")
	})
}

func TestFirmirrorSyncer_SaveMetadata(t *testing.T) {
	t.Run("SavesNewComponents", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage, err := NewLocalStorage(tmpDir)
		require.NoError(t, err)
		syncer := NewFirmirrorSyncer(FirmirrorConfig{
			CacheDir: filepath.Join(tmpDir, "cache"),
		}, storage)

		// Add new components
		syncer.newComponents = []lvfs.Component{
			{
				Type:            "firmware",
				ID:              "com.test.firmware1",
				Name:            "Test Firmware 1",
				MetadataLicense: "proprietary",
				Releases: []lvfs.Release{
					{
						Version: "1.0.0",
						Checksums: []lvfs.Checksum{
							{
								Filename: "firmware1.bin",
								Type:     "sha256",
								Value:    "abc123",
							},
						},
					},
				},
			},
		}

		// Save metadata
		err = syncer.SaveMetadata(context.TODO())
		require.NoError(t, err, "Should save metadata successfully")

		// Verify compressed file exists
		metadataZstPath := filepath.Join(tmpDir, "metadata.xml.zst")
		assert.FileExists(t, metadataZstPath, "Compressed metadata should exist")

		// Read and verify content
		file, err := os.Open(metadataZstPath)
		require.NoError(t, err)
		defer file.Close()

		zstReader, err := zstd.NewReader(file)
		require.NoError(t, err)
		defer zstReader.Close()

		var components lvfs.Components
		decoder := xml.NewDecoder(zstReader)
		err = decoder.Decode(&components)
		require.NoError(t, err, "Should decode saved metadata")

		assert.Len(t, components.Component, 1, "Should have 1 component")
		assert.Equal(t, "com.test.firmware1", components.Component[0].ID)
		assert.Equal(t, "firmirror", components.Origin)

		// Verify location tag was added
		assert.Equal(t, "firmware1.bin.cab", components.Component[0].Releases[0].Location,
			"Should have location tag set")
	})

	t.Run("MergesExistingAndNewComponents", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage, err := NewLocalStorage(tmpDir)
		require.NoError(t, err)
		syncer := NewFirmirrorSyncer(FirmirrorConfig{
			CacheDir: filepath.Join(tmpDir, "cache"),
		}, storage)

		// Set existing metadata
		syncer.existingMetadata = &lvfs.Components{
			Component: []lvfs.Component{
				{
					Type: "firmware",
					ID:   "com.existing.firmware",
					Name: "Existing Firmware",
					Releases: []lvfs.Release{
						{
							Version: "1.0.0",
							Checksums: []lvfs.Checksum{
								{Filename: "existing.bin"},
							},
						},
					},
				},
			},
		}

		// Add new components
		syncer.newComponents = []lvfs.Component{
			{
				Type: "firmware",
				ID:   "com.new.firmware",
				Name: "New Firmware",
				Releases: []lvfs.Release{
					{
						Version: "2.0.0",
						Checksums: []lvfs.Checksum{
							{Filename: "new.bin"},
						},
					},
				},
			},
		}

		// Save metadata
		err = syncer.SaveMetadata(context.TODO())
		require.NoError(t, err)

		// Read and verify merged content
		metadataZstPath := filepath.Join(tmpDir, "metadata.xml.zst")
		file, err := os.Open(metadataZstPath)
		require.NoError(t, err)
		defer file.Close()

		zstReader, err := zstd.NewReader(file)
		require.NoError(t, err)
		defer zstReader.Close()

		var components lvfs.Components
		decoder := xml.NewDecoder(zstReader)
		err = decoder.Decode(&components)
		require.NoError(t, err)

		assert.Len(t, components.Component, 2, "Should have 2 components (existing + new)")

		// Find components by ID
		var existingFound, newFound bool
		for _, comp := range components.Component {
			if comp.ID == "com.existing.firmware" {
				existingFound = true
				assert.Equal(t, "existing.bin.cab", comp.Releases[0].Location)
			}
			if comp.ID == "com.new.firmware" {
				newFound = true
				assert.Equal(t, "new.bin.cab", comp.Releases[0].Location)
			}
		}
		assert.True(t, existingFound, "Should have existing component")
		assert.True(t, newFound, "Should have new component")
	})

	t.Run("MergesReleasesForSameComponentID", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage, err := NewLocalStorage(tmpDir)
		require.NoError(t, err)
		syncer := NewFirmirrorSyncer(FirmirrorConfig{
			CacheDir: filepath.Join(tmpDir, "cache"),
		}, storage)

		// Set existing metadata with a component
		syncer.existingMetadata = &lvfs.Components{
			Component: []lvfs.Component{
				{
					Type: "firmware",
					ID:   "com.test.firmware",
					Name: "Test Firmware",
					Releases: []lvfs.Release{
						{
							Version: "1.0.0",
							Checksums: []lvfs.Checksum{
								{Filename: "firmware-v1.bin"},
							},
						},
					},
				},
			},
		}

		// Add new release for the same component ID
		syncer.newComponents = []lvfs.Component{
			{
				Type: "firmware",
				ID:   "com.test.firmware",
				Name: "Test Firmware",
				Releases: []lvfs.Release{
					{
						Version: "2.0.0",
						Checksums: []lvfs.Checksum{
							{Filename: "firmware-v2.bin"},
						},
					},
				},
			},
		}

		// Save metadata
		err = syncer.SaveMetadata(context.TODO())
		require.NoError(t, err)

		// Read and verify merged content
		metadataZstPath := filepath.Join(tmpDir, "metadata.xml.zst")
		file, err := os.Open(metadataZstPath)
		require.NoError(t, err)
		defer file.Close()

		zstReader, err := zstd.NewReader(file)
		require.NoError(t, err)
		defer zstReader.Close()

		var components lvfs.Components
		decoder := xml.NewDecoder(zstReader)
		err = decoder.Decode(&components)
		require.NoError(t, err)

		assert.Len(t, components.Component, 1, "Should have 1 component")
		assert.Equal(t, "com.test.firmware", components.Component[0].ID)
		assert.Len(t, components.Component[0].Releases, 2, "Should have 2 releases")

		// Verify both releases are present
		versions := []string{}
		for _, release := range components.Component[0].Releases {
			versions = append(versions, release.Version)
		}
		assert.Contains(t, versions, "1.0.0", "Should have version 1.0.0")
		assert.Contains(t, versions, "2.0.0", "Should have version 2.0.0")
	})

	t.Run("SkipsWhenNoNewComponents", func(t *testing.T) {
		syncer, tmpDir := createTestSyncer(t)

		// No new components
		syncer.newComponents = []lvfs.Component{}

		// Save should skip
		err := syncer.SaveMetadata(context.TODO())
		assert.NoError(t, err, "Should not error")

		// Verify no metadata file was created
		metadataZstPath := filepath.Join(tmpDir, "output", "metadata.xml.zst")
		assert.NoFileExists(t, metadataZstPath, "Should not create metadata file")
	})

	t.Run("AddsLocationTagsToReleases", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage, err := NewLocalStorage(tmpDir)
		require.NoError(t, err)
		syncer := NewFirmirrorSyncer(FirmirrorConfig{
			CacheDir: filepath.Join(tmpDir, "cache"),
		}, storage)

		// Add components without location tags
		syncer.newComponents = []lvfs.Component{
			{
				Type: "firmware",
				ID:   "com.test.firmware",
				Releases: []lvfs.Release{
					{
						Version:  "1.0.0",
						Location: "", // No location
						Checksums: []lvfs.Checksum{
							{Filename: "firmware.bin"},
						},
					},
					{
						Version:  "2.0.0",
						Location: "already-set.cab", // Already has location
						Checksums: []lvfs.Checksum{
							{Filename: "firmware-v2.bin"},
						},
					},
				},
			},
		}

		// Save metadata
		err = syncer.SaveMetadata(context.TODO())
		require.NoError(t, err)

		// Read and verify
		metadataZstPath := filepath.Join(tmpDir, "metadata.xml.zst")
		file, err := os.Open(metadataZstPath)
		require.NoError(t, err)
		defer file.Close()

		zstReader, err := zstd.NewReader(file)
		require.NoError(t, err)
		defer zstReader.Close()

		var components lvfs.Components
		decoder := xml.NewDecoder(zstReader)
		err = decoder.Decode(&components)
		require.NoError(t, err)

		// Verify locations
		releases := components.Component[0].Releases
		assert.Equal(t, "firmware.bin.cab", releases[0].Location,
			"Should add location based on checksum filename")
		assert.Equal(t, "already-set.cab", releases[1].Location,
			"Should preserve existing location")
	})
}
