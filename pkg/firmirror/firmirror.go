package firmirror

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path"

	"github.com/criteo/firmirror/pkg/lvfs"
)

type FirmirrorConfig struct {
	OutputDir string
	CacheDir  string
}

type FimirrorSyncer struct {
	Config  FirmirrorConfig
	vendors map[string]Vendor
}

func NewFimirrorSyncer(config FirmirrorConfig) *FimirrorSyncer {
	// Create cache directory if it doesn't exist
	if err := os.MkdirAll(config.CacheDir, 0755); err != nil {
		slog.Error("Failed to create cache directory", "dir", config.CacheDir, "error", err)
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
		slog.Error("Failed to create output directory", "dir", config.OutputDir, "error", err)
	}

	return &FimirrorSyncer{
		Config:  config,
		vendors: make(map[string]Vendor),
	}
}

// RegisterVendor registers a vendor with the given name
func (f *FimirrorSyncer) RegisterVendor(name string, vendor Vendor) {
	f.vendors[name] = vendor
}

// GetAllVendors returns all registered vendors
func (f *FimirrorSyncer) GetAllVendors() map[string]Vendor {
	// Return a copy to prevent external modifications
	return maps.Clone(f.vendors)
}

// processVendor processes firmware for a given vendor using the interface
func (f *FimirrorSyncer) ProcessVendor(vendor Vendor, vendorName string) error {
	logger := slog.With("vendor", vendorName)
	logger.Info("Fetching catalog")

	catalog, err := vendor.FetchCatalog()
	if err != nil {
		logger.Error("Failed to fetch catalog", "error", err)
		return err
	}

	entries := catalog.ListEntries()
	processed := 0

	for _, entry := range entries {
		fwName := entry.GetFilename()
		entryLogger := logger.With("firmware", fwName)
		entryLogger.Info("Processing firmware")

		tmpDir := path.Join(f.Config.CacheDir, fwName+".wrk")
		if err := os.MkdirAll(tmpDir, 0755); err != nil {
			entryLogger.Error("Failed to create temp directory", "error", err)
			continue
		}

		if err = vendor.RetrieveFirmware(entry, tmpDir); err != nil {
			entryLogger.Error("Failed to retrieve firmware", "error", err)
			os.RemoveAll(tmpDir) // Clean up on error
			continue
		}

		// Convert to AppStream
		appstream, err := entry.ToAppstream()
		if err != nil {
			entryLogger.Error("Failed to convert firmware", "error", err)
			continue
		}

		// Add source URL
		sourceURL := entry.GetSourceURL()
		if sourceURL != "" {
			appstream.URL = lvfs.URL{
				Type: "homepage",
				Text: sourceURL,
			}
		}

		// Build package
		if err = f.buildPackage(appstream, fwName, tmpDir); err != nil {
			entryLogger.Error("Failed to build package", "error", err)
			continue
		}
		os.RemoveAll(tmpDir)

		processed++
		entryLogger.Info("Successfully processed firmware")
	}

	logger.Info("Completed vendor processing", "processed", processed)
	return nil
}

func (f *FimirrorSyncer) buildPackage(appstream *lvfs.Component, fwFile, tmpDir string) error {
	fwPath := path.Join(tmpDir, fwFile)

	// Add checksums to all releases
	sha1Hash, sha256Hash, err := calculateChecksums(fwPath)
	if err != nil {
		return err
	}

	for i := range appstream.Releases {
		appstream.Releases[i].Checksums = []lvfs.Checksum{
			{
				Filename: fwFile,
				Target:   "content",
				Type:     "sha1",
				Value:    sha1Hash,
			},
			{
				Filename: fwFile,
				Target:   "content",
				Type:     "sha256",
				Value:    sha256Hash,
			},
		}
	}

	fwMeta := path.Join(tmpDir, "firmware.metainfo.xml")
	outBytes := []byte(xml.Header)
	xmlBytes, err := xml.MarshalIndent(appstream, "", "  ")
	if err != nil {
		return err
	}
	outBytes = append(outBytes, xmlBytes...)
	if err = os.WriteFile(fwMeta, outBytes, 0644); err != nil {
		return err
	}

	cabPath := path.Join(f.Config.OutputDir, fwFile+".cab")
	fwupdArgs := []string{"build-cabinet", cabPath, fwMeta, fwPath}
	cmd := exec.Command("fwupdtool", fwupdArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		slog.Error("Failed to build package", "error", err, "output", string(out))
		return err
	}

	return nil
}

func calculateChecksums(filepath string) (sha1Hash, sha256Hash string, err error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", "", err
	}
	defer file.Close()

	sha1Hasher := sha1.New()
	sha256Hasher := sha256.New()

	// Use MultiWriter to compute both hashes in one pass
	if _, err := io.Copy(io.MultiWriter(sha1Hasher, sha256Hasher), file); err != nil {
		return "", "", err
	}

	sha1Hash = hex.EncodeToString(sha1Hasher.Sum(nil))
	sha256Hash = hex.EncodeToString(sha256Hasher.Sum(nil))

	return sha1Hash, sha256Hash, nil
}
