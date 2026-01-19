package dell

import (
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockServer creates a test HTTP server that serves the test catalog
func mockServer(t *testing.T) *httptest.Server {
	mux := http.NewServeMux()

	// Serve the test catalog XML (gzipped)
	mux.HandleFunc("/catalog/catalog.xml.gz", func(w http.ResponseWriter, r *http.Request) {
		catalogPath := filepath.Join("testdata", "catalog.xml")
		content, err := os.ReadFile(catalogPath)
		if !assert.NoError(t, err, "Should be able to read test catalog") {
			http.Error(w, "Test catalog not found", http.StatusNotFound)
			return
		}

		// Convert to UTF-16 Little Endian with BOM as expected by Dell's parser
		// Add BOM for UTF-16LE
		utf16Content := []byte{0xFF, 0xFE} // BOM for UTF-16LE

		// Convert each byte to UTF-16LE
		for _, b := range content {
			utf16Content = append(utf16Content, b, 0x00)
		}

		// Serve as gzipped content
		w.Header().Set("Content-Type", "application/x-gzip")

		gzipWriter := gzip.NewWriter(w)
		defer gzipWriter.Close()

		_, err = gzipWriter.Write(utf16Content)
		if !assert.NoError(t, err, "Should be able to gzip catalog content") {
			return
		}
	})

	// Serve mock firmware files
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/catalog/catalog.xml.gz" {
			return // Already handled above
		}

		filename := filepath.Base(r.URL.Path)

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "attachment; filename="+filename)

		// Return mock firmware content
		mockContent := "Mock Dell firmware content for " + filename
		w.Write([]byte(mockContent))
	})

	return httptest.NewServer(mux)
}

func TestNewDellVendor(t *testing.T) {
	t.Run("WithSystemIDs", func(t *testing.T) {
		systemIDs := []string{"0C60", "0C61"}
		vendor := NewDellVendor(systemIDs)

		assert.NotNil(t, vendor, "Vendor should not be nil")
		assert.Equal(t, "https://dl.dell.com", vendor.BaseURL, "BaseURL should be set correctly")
		assert.Equal(t, systemIDs, vendor.SystemIDs, "SystemIDs should be set correctly")
	})

	t.Run("WithoutSystemIDs", func(t *testing.T) {
		vendor := NewDellVendor(nil)

		assert.NotNil(t, vendor, "Vendor should not be nil")
		assert.Equal(t, "https://dl.dell.com", vendor.BaseURL, "BaseURL should be set correctly")
		assert.Nil(t, vendor.SystemIDs, "SystemIDs should be nil")
	})

	t.Run("WithEmptySystemIDs", func(t *testing.T) {
		vendor := NewDellVendor([]string{})

		assert.NotNil(t, vendor, "Vendor should not be nil")
		assert.Equal(t, "https://dl.dell.com", vendor.BaseURL, "BaseURL should be set correctly")
		assert.Empty(t, vendor.SystemIDs, "SystemIDs should be empty")
	})
}

func TestDellVendor_FetchCatalog(t *testing.T) {
	server := mockServer(t)
	defer server.Close()

	t.Run("NoSystemIDFilter", func(t *testing.T) {
		vendor := &DellVendor{
			BaseURL:   server.URL,
			SystemIDs: nil, // No filter
		}

		catalog, err := vendor.FetchCatalog()
		assert.NoError(t, err, "FetchCatalog should not return an error")
		assert.NotNil(t, catalog, "Catalog should not be nil")

		dellCatalog, ok := catalog.(*DellCatalog)
		assert.True(t, ok, "Catalog should be of type *DellCatalog")

		// Should have 2 firmware entries (drivers should be filtered out)
		assert.Len(t, dellCatalog.SoftwareComponents, 2, "Should have 2 firmware components")

		// Verify only firmware components are included
		for _, component := range dellCatalog.SoftwareComponents {
			assert.Equal(t, "FRMW", component.ComponentType.Value, "Only firmware components should be included")
		}
	})

	t.Run("WithSystemIDFilter", func(t *testing.T) {
		vendor := &DellVendor{
			BaseURL:   server.URL,
			SystemIDs: []string{"0C60"}, // Filter for specific system
		}

		catalog, err := vendor.FetchCatalog()
		assert.NoError(t, err, "FetchCatalog should not return an error")
		assert.NotNil(t, catalog, "Catalog should not be nil")

		dellCatalog, ok := catalog.(*DellCatalog)
		assert.True(t, ok, "Catalog should be of type *DellCatalog")

		// Should have 2 entries (both firmware support 0C60)
		assert.Len(t, dellCatalog.SoftwareComponents, 2, "Should have 2 components for system 0C60")
	})

	t.Run("WithNonMatchingSystemIDFilter", func(t *testing.T) {
		vendor := &DellVendor{
			BaseURL:   server.URL,
			SystemIDs: []string{"9999"}, // Non-existing system
		}

		catalog, err := vendor.FetchCatalog()
		assert.NoError(t, err, "FetchCatalog should not return an error")
		assert.NotNil(t, catalog, "Catalog should not be nil")

		dellCatalog, ok := catalog.(*DellCatalog)
		assert.True(t, ok, "Catalog should be of type *DellCatalog")

		// Should have 0 entries
		assert.Len(t, dellCatalog.SoftwareComponents, 0, "Should have 0 components for non-matching system")
	})
}

func TestDellVendor_RetrieveFirmware(t *testing.T) {
	server := mockServer(t)
	defer server.Close()

	vendor := &DellVendor{
		BaseURL: server.URL,
	}

	tmpDir := t.TempDir()

	// Create a test firmware entry
	entry := &DellFirmwareEntry{
		Filename: "firmware1.exe",
		DellSoftwareComponent: &DellSoftwareComponent{
			Path: "FOLDER01/firmware1.exe",
			Name: DellTranslatable{
				Display: []DellTranslatableEntry{
					{Lang: "en", Value: "Test Firmware"},
				},
			},
		},
	}

	// Test retrieving firmware
	err := vendor.RetrieveFirmware(entry, tmpDir)
	assert.NoError(t, err, "RetrieveFirmware should not return an error")

	// Check that file was created
	expectedPath := filepath.Join(tmpDir, "firmware1.exe")
	assert.FileExists(t, expectedPath, "Downloaded file should exist")

	// Verify file content
	content, err := os.ReadFile(expectedPath)
	assert.NoError(t, err, "Should be able to read downloaded file")

	expectedContent := "Mock Dell firmware content for firmware1.exe"
	assert.Equal(t, expectedContent, string(content), "File content should match expected")
}

func TestDellCatalog_ListEntries(t *testing.T) {
	catalog := &DellCatalog{
		SoftwareComponents: []DellSoftwareComponent{
			{
				Path: "folder1/firmware1.exe",
				Name: DellTranslatable{
					Display: []DellTranslatableEntry{
						{Lang: "en", Value: "First Firmware"},
					},
				},
				ComponentType: DellTranslatableWithValue{Value: "FRMW"},
			},
			{
				Path: "folder2/firmware2.exe",
				Name: DellTranslatable{
					Display: []DellTranslatableEntry{
						{Lang: "en", Value: "Second Firmware"},
					},
				},
				ComponentType: DellTranslatableWithValue{Value: "FRMW"},
			},
		},
	}

	entries := catalog.ListEntries()
	assert.Len(t, entries, 2, "Should return exactly 2 entries")

	// Check that entries are properly converted
	filenames := make([]string, len(entries))
	for i, entry := range entries {
		dellEntry, ok := entry.(*DellFirmwareEntry)
		assert.True(t, ok, "Entry should be of type *DellFirmwareEntry")
		assert.NotNil(t, dellEntry.DellSoftwareComponent, "DellSoftwareComponent field should not be nil")

		filenames[i] = dellEntry.GetFilename()
	}

	assert.Contains(t, filenames, "firmware1.exe", "Should contain firmware1.exe")
	assert.Contains(t, filenames, "firmware2.exe", "Should contain firmware2.exe")
}

func TestDellFirmwareEntry_GetFilename(t *testing.T) {
	entry := &DellFirmwareEntry{
		Filename:              "test-firmware.exe",
		DellSoftwareComponent: &DellSoftwareComponent{},
	}

	filename := entry.GetFilename()
	assert.Equal(t, "test-firmware.exe", filename, "GetFilename should return the correct filename")
}

func TestDellFirmwareEntry_ToAppstream(t *testing.T) {
	entry := &DellFirmwareEntry{
		Filename: "test-firmware.exe",
		DellSoftwareComponent: &DellSoftwareComponent{
			Path:           "FOLDER01/test-firmware.exe",
			VendorVersion:  "1.0.0",
			DateTime:       mustParseTime("2024-01-15T10:30:00Z"),
			RebootRequired: true,
			Name: DellTranslatable{
				Display: []DellTranslatableEntry{
					{Lang: "en", Value: "Test Network Firmware"},
				},
			},
			Description: DellTranslatable{
				Display: []DellTranslatableEntry{
					{Lang: "en", Value: "Test firmware description"},
				},
			},
			ImportantInfo: DellTranslatable{
				Display: []DellTranslatableEntry{
					{Lang: "en", Value: "Reboot required"},
				},
			},
			LUCategory: DellTranslatableWithValue{
				Value: "Network",
			},
			Criticality: DellCriticality{
				Value: 1, // Medium urgency
			},
			SupportedSystems: []DellBrand{
				{
					Models: []DellModel{
						{SystemID: "0C60"},
					},
				},
			},
			SupportedDevices: []DellDevice{
				{ComponentID: "DEV001"},
			},
		},
	}

	component, err := entry.ToAppstream()
	assert.NoError(t, err, "ToAppstream should not return an error")
	assert.NotNil(t, component, "Component should not be nil")

	// Verify basic component properties
	assert.Equal(t, "firmware", component.Type, "Component type should be firmware")
	assert.Equal(t, "proprietary", component.MetadataLicense, "Metadata license should be proprietary")
	assert.Equal(t, "proprietary", component.ProjectLicense, "Project license should be proprietary")
	assert.Equal(t, "Test Network Firmware", component.Name, "Component name should match")
	assert.Equal(t, "Test firmware description", component.Summary, "Component summary should match")

	// Verify releases
	assert.Len(t, component.Releases, 1, "Should have exactly one release")
	release := component.Releases[0]
	assert.Equal(t, "1.0.0", release.Version, "Release version should match")
	assert.Equal(t, "medium", release.Urgency, "Release urgency should be medium for criticality 1")

	// Verify categories for Network LUCategory
	assert.Contains(t, component.Categories, "X-NetworkInterface", "Should contain X-NetworkInterface category")

	// Verify custom fields for reboot required
	customKeys := make([]string, len(component.Custom))
	for i, custom := range component.Custom {
		customKeys[i] = custom.Key
	}
	assert.Contains(t, customKeys, "LVFS::DeviceFlags", "Should contain DeviceFlags custom field")
	assert.Contains(t, customKeys, "LVFS::UpdateMessage", "Should contain UpdateMessage custom field")
	assert.Contains(t, customKeys, "LVFS::UpdateProtocol", "Should contain UpdateProtocol custom field")
	assert.Contains(t, customKeys, "LVFS::DeviceIntegrity", "Should contain DeviceIntegrity custom field")

	// Verify provides section
	assert.NotEmpty(t, component.Provides, "Should have provides entries")
}

// Helper function for parsing time in tests
func mustParseTime(timeStr string) time.Time {
	t, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		panic(err)
	}
	return t
}
