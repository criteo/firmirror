package firmirror

import (
	"encoding/xml"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path"

	"github.com/criteo/firmirror/pkg/lvfs"
)

type FirmirrorConfig struct {
	OutputDir string
}

type FimirrorSyncer struct {
	Config  FirmirrorConfig
	vendors map[string]Vendor
}

func NewFimirrorSyncer(config FirmirrorConfig) *FimirrorSyncer {
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

		tmpDir := path.Join(f.Config.OutputDir, fwName+".wrk")
		if err := os.MkdirAll(tmpDir, 0755); err != nil {
			entryLogger.Error("Failed to create temp directory", "error", err)
			continue
		}

		if err = vendor.RetrieveFirmware(entry, tmpDir); err != nil {
			entryLogger.Error("Failed to retrieve firmware", "error", err)
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
		err = f.buildPackage(appstream, tmpDir)
		if err != nil {
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

func (f *FimirrorSyncer) buildPackage(appstream *lvfs.Component, tmpDir string) error {
	outBytes := []byte(xml.Header)
	xmlBytes, err := xml.MarshalIndent(appstream, "", "  ")
	if err != nil {
		return err
	}
	outBytes = append(outBytes, xmlBytes...)

	fwFiles, err := os.ReadDir(tmpDir)
	if err != nil {
		return err
	}

	fwupdArgs := []string{"build-cabinet", fwFiles[0].Name() + ".cab", path.Join(tmpDir, "/firmware.metainfo.xml")}
	for _, f := range fwFiles {
		fwupdArgs = append(fwupdArgs, path.Join(tmpDir, f.Name()))
	}

	err = os.WriteFile(path.Join(tmpDir, "/firmware.metainfo.xml"), outBytes, 0644)
	if err != nil {
		return err
	}

	cmd := exec.Command("fwupdtool", fwupdArgs...)
	cmd.Dir = f.Config.OutputDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		slog.Error("Failed to build package", "error", err, "output", string(out))
		return err
	}

	return nil
}
