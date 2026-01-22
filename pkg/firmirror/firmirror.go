package firmirror

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/criteo/firmirror/pkg/lvfs"
	"github.com/klauspost/compress/zstd"
)

type FirmirrorConfig struct {
	OutputDir string
	CacheDir  string
}

type FimirrorSyncer struct {
	Config           FirmirrorConfig
	vendors          map[string]Vendor
	existingMetadata *lvfs.Components // Loaded metadata from existing metadata.xml.gz
	existingIndex    map[string]bool  // Index of firmware already in metadata (by filename)
	newComponents    []lvfs.Component // Components accumulated during this run
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
		Config:        config,
		vendors:       make(map[string]Vendor),
		existingIndex: make(map[string]bool),
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
func (f *FimirrorSyncer) ProcessVendor(ctx context.Context, vendor Vendor, vendorName string) error {
	logger := slog.With("vendor", vendorName)
	logger.Info("Fetching catalog")

	catalog, err := vendor.FetchCatalog()
	if err != nil {
		logger.Error("Failed to fetch catalog", "error", err)
		return err
	}

	entries := catalog.ListEntries()
	processed := 0
	skipped := 0

	for _, entry := range entries {
		// Stop if interruption raised
		// TODO: Trickle down to downloads as well
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fwName := entry.GetFilename()
		entryLogger := logger.With("firmware", fwName)

		// Check if firmware is already in metadata index
		if f.existingIndex[fwName] {
			entryLogger.Info("Firmware already in metadata index, skipping")
			skipped++
			continue
		}

		entryLogger.Info("Processing firmware")

		tmpDir := filepath.Join(f.Config.CacheDir, fwName+".wrk")
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

		// Accumulate component for metadata generation
		f.newComponents = append(f.newComponents, *appstream)

		processed++
		entryLogger.Info("Successfully processed firmware")
	}

	logger.Info("Completed vendor processing", "processed", processed, "skipped", skipped, "total", len(entries))
	return nil
}

func (f *FimirrorSyncer) buildPackage(appstream *lvfs.Component, fwFile, tmpDir string) error {
	fwPath := filepath.Join(tmpDir, fwFile)

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

	fwMeta := filepath.Join(tmpDir, "firmware.metainfo.xml")
	outBytes := []byte(xml.Header)
	xmlBytes, err := xml.MarshalIndent(appstream, "", "  ")
	if err != nil {
		return err
	}
	outBytes = append(outBytes, xmlBytes...)
	if err = os.WriteFile(fwMeta, outBytes, 0644); err != nil {
		return err
	}

	cabPath := filepath.Join(f.Config.OutputDir, fwFile+".cab")
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

// LoadMetadata loads existing metadata.xml.zst and builds an index of existing firmware
func (f *FimirrorSyncer) LoadMetadata() error {
	metadataPath := filepath.Join(f.Config.OutputDir, "metadata.xml.zst")

	// Check if metadata file exists
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		slog.Info("No existing metadata found, starting fresh")
		return nil
	}

	// Open and decompress metadata file
	file, err := os.Open(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer file.Close()

	zstReader, err := zstd.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer zstReader.Close()

	// Read and parse XML
	data, err := io.ReadAll(zstReader)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	var components lvfs.Components
	if err := xml.Unmarshal(data, &components); err != nil {
		return fmt.Errorf("failed to parse metadata XML: %w", err)
	}

	f.existingMetadata = &components

	// Build index of existing firmware files from checksums
	for _, comp := range components.Component {
		for _, release := range comp.Releases {
			for _, checksum := range release.Checksums {
				if checksum.Filename != "" {
					f.existingIndex[checksum.Filename] = true
				}
			}
		}
	}

	slog.Info("Loaded existing metadata",
		"components", len(components.Component),
		"firmware_files", len(f.existingIndex))

	return nil
}

// SaveMetadata saves the combined metadata (existing + accumulated) to metadata.xml.zst
func (f *FimirrorSyncer) SaveMetadata() error {
	logger := slog.With("component", "metadata-save")

	if len(f.newComponents) == 0 {
		logger.Info("No new component, skipping metadata update")
		return nil
	}

	componentMap := make(map[string]*lvfs.Component)

	// Add existing components first
	if f.existingMetadata != nil {
		for i := range f.existingMetadata.Component {
			comp := f.existingMetadata.Component[i]
			componentMap[comp.ID] = &comp
		}
	}

	// Add or merge new components
	for _, comp := range f.newComponents {
		if existing, ok := componentMap[comp.ID]; ok {
			// Merge releases if component already exists
			logger.Info("Merging component", "id", comp.ID)
			existing.Releases = append(existing.Releases, comp.Releases...)
		} else {
			// Add new component
			componentMap[comp.ID] = &comp
		}
	}

	// Build final components structure
	components := &lvfs.Components{
		Origin: "firmirror",
	}
	for _, component := range componentMap {
		// Ensure each release has a location tag
		for i := range component.Releases {
			release := &component.Releases[i]
			if release.Location == "" && len(release.Checksums) > 0 {
				// FIXME: this is brittle
				release.Location = release.Checksums[0].Filename + ".cab"
			}
		}
		components.Component = append(components.Component, *component)
	}

	// Write metadata using simple XML marshaling
	metadataPath := filepath.Join(f.Config.OutputDir, "metadata.xml")
	outBytes := []byte(xml.Header)
	xmlBytes, err := xml.MarshalIndent(components, "", "  ")
	if err != nil {
		return err
	}
	outBytes = append(outBytes, xmlBytes...)
	if err := os.WriteFile(metadataPath, outBytes, 0644); err != nil {
		return err
	}

	// Compress metadata
	if err := compressMetadata(metadataPath); err != nil {
		return err
	}

	logger.Info("Metadata saved successfully",
		"total_components", len(componentMap),
		"new_components", len(f.newComponents))

	return nil
}

func compressMetadata(filepath string) error {
	inputFile, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(filepath + ".zst")
	if err != nil {
		return err
	}
	defer outputFile.Close()

	zstWriter, err := zstd.NewWriter(outputFile)
	if err != nil {
		return err
	}
	defer zstWriter.Close()

	_, err = io.Copy(zstWriter, inputFile)
	if err != nil {
		return err
	}

	return nil
}
