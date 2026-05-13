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
	"sync"
	"time"

	"github.com/premday/firmirror/pkg/lvfs"
	"github.com/klauspost/compress/zstd"
)

type FirmirrorConfig struct {
	CacheDir       string // Local cache directory for temporary work
	Certificate    string // Path to certificate file for signing metadata (.pem or .crt)
	PrivateKey     string // Path to private key file for signing metadata (.pem or .key)
	MaxConcurrency int    // Maximum number of firmware entries processed concurrently (default 1)
}

type FirmirrorSyncer struct {
	Config           FirmirrorConfig
	Storage          Storage
	vendors          map[string]Vendor
	existingMetadata *lvfs.Components // Loaded metadata from existing metadata.xml.gz
	existingIndex    map[string]bool  // Index of firmware already in metadata (by filename)
	newComponents    []lvfs.Component // Components accumulated during this run
}

func NewFirmirrorSyncer(config FirmirrorConfig, storage Storage) *FirmirrorSyncer {
	// Create cache directory if it doesn't exist
	if err := os.MkdirAll(config.CacheDir, 0755); err != nil {
		slog.Error("Failed to create cache directory", "dir", config.CacheDir, "error", err)
	}

	return &FirmirrorSyncer{
		Config:        config,
		Storage:       storage,
		vendors:       make(map[string]Vendor),
		existingIndex: make(map[string]bool),
	}
}

// RegisterVendor registers a vendor with the given name
func (f *FirmirrorSyncer) RegisterVendor(name string, vendor Vendor) {
	f.vendors[name] = vendor
}

// GetAllVendors returns all registered vendors
func (f *FirmirrorSyncer) GetAllVendors() map[string]Vendor {
	// Return a copy to prevent external modifications
	return maps.Clone(f.vendors)
}

// ProcessVendor processes firmware for a given vendor using the interface.
// Entries are processed concurrently up to Config.MaxConcurrency workers.
func (f *FirmirrorSyncer) ProcessVendor(ctx context.Context, vendor Vendor, vendorName string) error {
	logger := slog.With("vendor", vendorName)
	logger.Debug("Fetching catalog")

	catalog, err := vendor.FetchCatalog(ctx)
	if err != nil {
		logger.Error("Failed to fetch catalog", "error", err)
		return err
	}

	entries := catalog.ListEntries()

	sem := make(chan struct{}, f.Config.MaxConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	processed := 0
	skipped := 0

	for _, entry := range entries {
		fwName := entry.GetFilename()

		// Check if firmware is already in metadata index (read-only map, safe for concurrent reads)
		if f.existingIndex[fwName] {
			logger.With("firmware", fwName).Info("Firmware already in metadata index, skipping")
			skipped++
			continue
		}

		// Acquire semaphore slot, respecting context cancellation
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			components := f.processEntry(ctx, vendor, entry, fwName, logger)
			if components == nil {
				return
			}

			mu.Lock()
			f.newComponents = append(f.newComponents, components...)
			processed++
			mu.Unlock()
		}()
	}

	wg.Wait()

	logger.Info("Completed vendor processing", "processed", processed, "skipped", skipped, "total", len(entries))
	return nil
}

// processEntry handles downloading, converting and packaging a single firmware entry.
// Returns the resulting components, or nil on error.
func (f *FirmirrorSyncer) processEntry(ctx context.Context, vendor Vendor, entry FirmwareEntry, fwName string, logger *slog.Logger) []lvfs.Component {
	entryLogger := logger.With("firmware", fwName)
	entryLogger.Info("Processing firmware")
	start := time.Now()

	tmpDir := filepath.Join(f.Config.CacheDir, fwName+".wrk")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		entryLogger.Error("Failed to create temp directory", "error", err)
		return nil
	}

	t0 := time.Now()
	if err := vendor.RetrieveFirmware(ctx, entry, tmpDir); err != nil {
		entryLogger.Error("Failed to retrieve firmware", "error", err)
		os.RemoveAll(tmpDir)
		return nil
	}
	entryLogger.Info("Downloaded firmware", "duration", time.Since(t0).Round(time.Millisecond))

	// Convert to AppStream components (one firmware entry may produce multiple components)
	components, err := entry.ToAppstream()
	if err != nil {
		entryLogger.Error("Failed to convert firmware", "error", err)
		os.RemoveAll(tmpDir)
		return nil
	}

	if len(components) == 0 {
		entryLogger.Info("No components produced, skipping")
		os.RemoveAll(tmpDir)
		return nil
	}

	sourceURL := entry.GetSourceURL()
	componentPtrs := make([]*lvfs.Component, len(components))
	for i := range components {
		componentPtrs[i] = &components[i]
		if sourceURL != "" {
			componentPtrs[i].URL = lvfs.URL{
				Type: "homepage",
				Text: sourceURL,
			}
		}
	}

	// Build a single package containing all component metainfo XMLs
	t0 = time.Now()
	if err = f.buildPackage(ctx, componentPtrs, fwName, tmpDir); err != nil {
		entryLogger.Error("Failed to build package", "error", err)
		os.RemoveAll(tmpDir)
		return nil
	}
	entryLogger.Info("Built and uploaded package", "duration", time.Since(t0).Round(time.Millisecond))
	os.RemoveAll(tmpDir)

	result := make([]lvfs.Component, len(componentPtrs))
	for i, comp := range componentPtrs {
		result[i] = *comp
	}

	entryLogger.Info("Completed firmware", "total_duration", time.Since(start).Round(time.Millisecond))
	return result
}

func (f *FirmirrorSyncer) buildPackage(ctx context.Context, components []*lvfs.Component, fwFile, tmpDir string) error {
	fwPath := filepath.Join(tmpDir, fwFile)
	logger := slog.With("firmware", fwFile)

	// Calculate firmware checksums (shared by all components)
	sha1Hash, sha256Hash, err := calculateChecksums(fwPath)
	if err != nil {
		return err
	}

	// Write a metainfo XML per component, add firmware checksums to each
	var metainfoPaths []string
	for i, component := range components {
		for j := range component.Releases {
			component.Releases[j].Checksums = []lvfs.Checksum{
				{Filename: fwFile, Target: "content", Type: "sha1", Value: sha1Hash},
				{Filename: fwFile, Target: "content", Type: "sha256", Value: sha256Hash},
			}
		}

		metaName := fmt.Sprintf("%d.metainfo.xml", i)
		metaPath := filepath.Join(tmpDir, metaName)
		outBytes := []byte(xml.Header)
		xmlBytes, err := xml.MarshalIndent(component, "", "  ")
		if err != nil {
			return err
		}
		outBytes = append(outBytes, xmlBytes...)
		if err = os.WriteFile(metaPath, outBytes, 0644); err != nil {
			return err
		}
		metainfoPaths = append(metainfoPaths, metaPath)
	}

	fwSig := filepath.Join(tmpDir, "firmware.jcat")
	// sign payload
	if err := f.signMetadata(fwSig, fwPath); err != nil {
		return err
	}
	// sign each metainfo
	for _, metaPath := range metainfoPaths {
		if err := f.signMetadata(fwSig, metaPath); err != nil {
			return err
		}
	}

	// Build CAB with firmware file, all metainfo XMLs, and jcat at the end
	cabBaseName := fwFile + ".cab"
	cabPathInCache := filepath.Join(tmpDir, cabBaseName)
	fwupdArgs := []string{"build-cabinet", cabPathInCache, fwPath}
	fwupdArgs = append(fwupdArgs, metainfoPaths...)
	fwupdArgs = append(fwupdArgs, fwSig)
	cmd := exec.Command("fwupdtool", fwupdArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error("Failed to build package", "error", err, "output", string(out))
		return err
	}

	// Calculate CAB checksums (shared by all components)
	cabSha1, cabSha256, err := calculateChecksums(cabPathInCache)
	if err != nil {
		return fmt.Errorf("failed to calculate CAB checksums: %w", err)
	}

	cabName := cabSha256 + "-" + fwFile + ".cab"
	// Add artifacts section to all components
	for _, component := range components {
		for i := range component.Releases {
			component.Releases[i].Artifacts = []lvfs.Artifact{
				{
					Type:     "binary",
					Location: cabName,
					Checksums: []lvfs.Checksum{
						{Type: "sha1", Value: cabSha1},
						{Type: "sha256", Value: cabSha256},
					},
				},
			}
		}
	}

	// Write CAB to storage backend
	cabFile, err := os.Open(cabPathInCache)
	if err != nil {
		return fmt.Errorf("failed to open CAB file: %w", err)
	}
	defer cabFile.Close()

	if err := f.Storage.Write(ctx, cabName, cabFile); err != nil {
		return fmt.Errorf("failed to write CAB to storage: %w", err)
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
func (f *FirmirrorSyncer) LoadMetadata(ctx context.Context) error {
	metadataKey := "metadata.xml.zst"

	// Check if metadata file exists
	exists, err := f.Storage.Exists(ctx, metadataKey)
	if err != nil {
		return fmt.Errorf("failed to check metadata existence: %w", err)
	}
	if !exists {
		slog.Info("No existing metadata found, starting fresh")
		return nil
	}

	// Read metadata from storage
	reader, err := f.Storage.Read(ctx, metadataKey)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}
	defer reader.Close()

	zstReader, err := zstd.NewReader(reader)
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
func (f *FirmirrorSyncer) SaveMetadata(ctx context.Context) error {
	ctx = context.WithoutCancel(ctx)
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
			logger.Debug("Merging component", "id", comp.ID)
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
			if release.Location == "" && len(release.Artifacts) > 0 && release.Artifacts[0].Location != "" {
				release.Location = release.Artifacts[0].Location
			}
		}
		components.Component = append(components.Component, *component)
	}

	// Marshal metadata to XML
	outBytes := []byte(xml.Header)
	xmlBytes, err := xml.MarshalIndent(components, "", "  ")
	if err != nil {
		return err
	}
	outBytes = append(outBytes, xmlBytes...)

	// Write uncompressed metadata to temporary file for compression
	metadataPath := filepath.Join(f.Config.CacheDir, "metadata.xml")
	if err := os.WriteFile(metadataPath, outBytes, 0644); err != nil {
		return err
	}
	defer os.Remove(metadataPath)

	// Compress metadata
	compressedPath := metadataPath + ".zst"
	if err := compressMetadata(metadataPath); err != nil {
		return err
	}
	defer os.Remove(compressedPath)

	// Sign metadata
	signaturePath := compressedPath + ".jcat"
	if err := f.signMetadata(signaturePath, compressedPath); err != nil {
		return err
	}
	defer os.Remove(signaturePath)

	// Write compressed metadata to storage
	for _, filePath := range []string{compressedPath, signaturePath} {
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()
		if err := f.Storage.Write(ctx, filepath.Base(filePath), file); err != nil {
			return fmt.Errorf("failed to write file to storage: %w", err)
		}
	}

	logger.Info("Metadata saved successfully",
		"total_merged_components", len(componentMap),
		"new_components", len(f.newComponents))

	return nil
}

func compressMetadata(filePath string) error {
	inputFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(filePath + ".zst")
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

// signMetadata creates a JCAT signature file for the given file using jcat-tool
// The jcat file contains checksums (SHA256, SHA512) and signature if signing keys are provided
func (f *FirmirrorSyncer) signMetadata(sigPath, filePath string) error {
	jcatTool := func(args []string, wd string) error {
		slog.Debug("Running jcat-tool", "args", args)
		cmd := exec.Command("jcat-tool", args...)
		cmd.Dir = wd
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("jcat-tool failed: %w\nOutput: %s", err, output)
		}
		return nil
	}

	wd := filepath.Dir(filePath)
	file := filepath.Base(filePath)
	sig := filepath.Base(sigPath)

	// Create JCAT file with checksum
	if err := jcatTool([]string{"self-sign", sig, file, "--kind", "sha256"}, wd); err != nil {
		return fmt.Errorf("failed to create JCAT file with checksums: %w", err)
	}

	// Add signature to JCAT file using certificate and private key
	// with GPG:
	//   gpg --detach-sign --sign --armor firmware.xml.zst
	//   jcat-tool import firmware.xml.zst.jcat firmware.xml.zst firmware.xml.zst.asc
	if f.Config.Certificate != "" && f.Config.PrivateKey != "" {
		if err := jcatTool([]string{"sign", sig, file, f.Config.Certificate, f.Config.PrivateKey}, wd); err != nil {
			return fmt.Errorf("failed to add signature to JCAT file: %w", err)
		}
	}

	return nil
}
