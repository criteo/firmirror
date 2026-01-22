package dell

import (
	"compress/gzip"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/criteo/firmirror/pkg/firmirror"
	"github.com/criteo/firmirror/pkg/lvfs"
	"github.com/criteo/firmirror/pkg/utils"
	"github.com/google/uuid"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

func NewDellVendor(systemIDs []string) *DellVendor {
	vendor := &DellVendor{
		BaseURL:   "https://dl.dell.com",
		SystemIDs: systemIDs,
	}

	return vendor
}

func (dv *DellVendor) FetchCatalog() (firmirror.Catalog, error) {
	catalog, err := dv.fetchCatalog()
	if err != nil {
		return nil, err
	}

	// Filter catalog entries based on vendor settings
	filteredCatalog := dv.filterCatalog(catalog)
	return filteredCatalog, nil
}

func (dv *DellVendor) fetchCatalog() (*DellCatalog, error) {
	catalogBody, err := utils.DownloadFile(dv.BaseURL + "/catalog/catalog.xml.gz")
	if err != nil {
		return nil, err
	}
	defer catalogBody.Close()

	rawCatalog, err := gzip.NewReader(catalogBody)
	if err != nil {
		return nil, err
	}
	defer rawCatalog.Close()

	// The XML decoder only reads UTF-8, so we need to convert the UTF-16 to UTF-8
	unicodeReader := transform.NewReader(rawCatalog, unicode.BOMOverride(unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder()))

	var dellCatalog DellCatalog

	xmlDecoder := xml.NewDecoder(unicodeReader)
	xmlDecoder.CharsetReader = func(charset string, input io.Reader) (io.Reader, error) {
		// But then we need to ignore the charset in the XML declaration
		// because the XML decoder will still try to read it as UTF-8
		// and fail if it's not
		return input, nil
	}
	err = xmlDecoder.Decode(&dellCatalog)
	if err != nil {
		return nil, err
	}

	return &dellCatalog, nil
}

func (dv *DellVendor) filterCatalog(catalog *DellCatalog) *DellCatalog {
	filteredComponents := []DellSoftwareComponent{}

	for _, fw := range catalog.SoftwareComponents {
		// Only select firmware, not drivers
		// FIXME include BIOS ?
		if fw.ComponentType.Value != "FRMW" {
			continue
		}

		// If no SystemIDs filter is set, include all firmware
		if len(dv.SystemIDs) == 0 {
			filteredComponents = append(filteredComponents, fw)
			continue
		}

	systemLoop:
		for _, system := range fw.SupportedSystems {
			for _, model := range system.Models {
				if slices.Contains(dv.SystemIDs, model.SystemID) {
					filteredComponents = append(filteredComponents, fw)
					break systemLoop
				}
			}
		}
	}

	filteredCatalog := *catalog // Copy the catalog
	filteredCatalog.SoftwareComponents = filteredComponents
	return &filteredCatalog
}

func (dv *DellVendor) RetrieveFirmware(entry firmirror.FirmwareEntry, tmpDir string) error {
	dellEntry, ok := entry.(*DellFirmwareEntry)
	if !ok {
		return fmt.Errorf("invalid entry type for Dell vendor")
	}

	fwPath := dellEntry.DellSoftwareComponent.Path
	filepath := filepath.Join(tmpDir, filepath.Base(fwPath))
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		if err := utils.DownloadFileToDest(dv.BaseURL+"/"+fwPath, filepath); err != nil {
			return err
		}
	}

	return nil
}

func (dc *DellCatalog) ListEntries() []firmirror.FirmwareEntry {
	entries := []firmirror.FirmwareEntry{}
	for _, fw := range dc.SoftwareComponents {
		entries = append(entries, &DellFirmwareEntry{
			Filename:              filepath.Base(fw.Path),
			DellSoftwareComponent: &fw,
			SourceURL:             dc.BaseLocation + "/" + fw.Path,
		})
	}
	return entries
}

func (dfe *DellFirmwareEntry) GetFilename() string {
	return dfe.Filename
}

func (dfe *DellFirmwareEntry) GetSourceURL() string {
	return dfe.SourceURL
}

func (dfe *DellFirmwareEntry) ToAppstream() (*lvfs.Component, error) {
	return processFirmware(*dfe.DellSoftwareComponent)
}

func processFirmware(fw DellSoftwareComponent) (*lvfs.Component, error) {
	out := lvfs.Component{
		Type:            "firmware",
		MetadataLicense: "proprietary",
		ProjectLicense:  "proprietary",
	}

	out.Name, _ = getString(fw.Name, "en")
	out.ID = fmt.Sprintf("com.%s.%s", strings.ToLower("Dell"), uuid.NewSHA1(uuid.NameSpaceDNS, []byte(out.Name)).String())

	for _, brand := range fw.SupportedSystems {
		for _, system := range brand.Models {
			for _, dev := range fw.SupportedDevices {
				out.Provides = append(out.Provides, lvfs.Firmware{
					Type: "flashed",
					Text: uuid.NewSHA1(uuid.NameSpaceDNS, fmt.Appendf(nil, "REDFISH\\VENDOR_Dell&SYSTEMID_%s&SOFTWAREID_%s", system.SystemID, dev.ComponentID)).String(),
				})
			}
		}
	}

	if fw.RebootRequired {
		out.Custom = append(out.Custom, lvfs.Custom{
			Key:   "LVFS::DeviceFlags",
			Value: "skips-restart",
		})
		rebootMessage, err := getString(fw.ImportantInfo, "en")
		if err != nil {
			return nil, err
		}

		out.Custom = append(out.Custom, lvfs.Custom{
			Key:   "LVFS::UpdateMessage",
			Value: rebootMessage,
		})
	}

	summary, err := getString(fw.Description, "en")
	if err != nil {
		return nil, err
	}
	out.Summary = summary
	out.Description = lvfs.Description{
		Value: "<p>" + summary + "</p>",
	}

	out.Releases = append(out.Releases, lvfs.Release{
		Version:     fw.VendorVersion,
		Date:        fw.DateTime.Format(time.DateOnly),
		Description: out.Description,
		Urgency:     getUrgency(fw.Criticality.Value),
	})

	switch fw.LUCategory.Value {
	case "BIOS":
		out.Categories = append(out.Categories, "X-System")
	case "Serial ATA", "SAS Drive":
		out.Categories = append(out.Categories, "X-Drive")
	case "Express Flash PCIe SSD":
		out.Categories = append(out.Categories, "X-SolidStateDrive")
	case "Network":
		out.Categories = append(out.Categories, "X-NetworkInterface")
	case "Chassis System Management":
		out.Categories = append(out.Categories, "X-Controller")
	case "iDRAC with Lifecycle Controller":
		out.Categories = append(out.Categories, "X-BaseboardManagementController")
	}

	out.Custom = append(out.Custom, lvfs.Custom{
		Key:   "LVFS::UpdateProtocol",
		Value: "org.dmtf.redfish",
	}, lvfs.Custom{
		Key: "LVFS::DeviceIntegrity",
		// All Dell firmware going through Redfish are signed
		Value: "signed",
	})

	return &out, nil
}

func getString(strings DellTranslatable, language string) (string, error) {
	for _, l := range strings.Display {
		if l.Lang == language {
			return l.Value, nil
		}
	}
	return "", fmt.Errorf("language not found: %s", language)
}

func getUrgency(criticality int64) string {
	switch criticality {
	case 1:
		return "medium"
	case 2:
		return "critical"
	case 3:
		return "low"
	default:
		return "medium"
	}
}
