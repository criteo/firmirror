package dell

import (
	"compress/gzip"
	"encoding/xml"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/criteo/firmirror/types"
	"github.com/criteo/firmirror/utils"
	"github.com/google/uuid"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

const CATALOG_URL = "https://dl.dell.com/catalog/catalog.xml.gz"

type DellFWCatalog struct {
	Catalog *DellCatalog
}

type DellFirmwareEntry struct {
	DellSoftwareComponent *DellSoftwareComponent
}

func (dc *DellFWCatalog) New(catalog *DellCatalog) *DellFWCatalog {
	fwCatalog := &DellFWCatalog{
		Catalog: catalog,
	}
	return fwCatalog
}

func (dc *DellFWCatalog) ListEntries() []DellFirmwareEntry {
	entries := []DellFirmwareEntry{}
	for _, fw := range dc.Catalog.SoftwareComponents {
		entries = append(entries, DellFirmwareEntry{
			DellSoftwareComponent: &fw,
		})
	}
	return entries
}

func (dfe *DellFirmwareEntry) GetFilename() string {
	return dfe.DellSoftwareComponent.HashMD5 + "-" + path.Base(dfe.DellSoftwareComponent.Path)
}

func dellGetString(strings DellTranslatable, language string) (string, error) {
	for _, l := range strings.Display {
		if l.Lang == language {
			return l.Value, nil
		}
	}
	return "", fmt.Errorf("language not found: %s", language)
}

func dellGetUrgency(criticality int64) string {
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

func DellFetchCatalog() (*DellCatalog, error) {
	catalogBody, err := utils.DownloadFile(CATALOG_URL)
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

func DellDownloadFirmware(fw DellSoftwareComponent, tmpDir string) (string, error) {
	file, err := utils.DownloadFile(fmt.Sprintf("https://dl.dell.com/%s", fw.Path))
	if err != nil {
		return "", err
	}
	defer file.Close()

	filepath := path.Join(tmpDir, path.Base(fw.Path))
	err = utils.ReaderToFile(file, filepath)
	if err != nil {
		return "", err
	}

	return filepath, nil
}

func HandleDellFirmware(fw DellSoftwareComponent) (*types.Component, error) {
	out := types.Component{}

	out.Type = "firmware"

	out.Name, _ = dellGetString(fw.Name, "en")
	out.ID = fmt.Sprintf("com.%s.%s", strings.ToLower("Dell"), uuid.NewSHA1(uuid.NameSpaceDNS, []byte(out.Name)).String())

	var guids []string
	for _, brand := range fw.SupportedSystems {
		for _, system := range brand.Models {
			for _, dev := range fw.SupportedDevices {
				guids = append(guids, uuid.NewSHA1(uuid.NameSpaceDNS, fmt.Appendf(nil, "REDFISH\\VENDOR_Dell&SYSTEMID_%s&SOFTWAREID_%s", system.SystemID, dev.ComponentID)).String())
			}
		}
	}

	for _, guid := range guids {
		out.Provides = append(out.Provides, types.Firmware{
			Type: "flashed",
			Text: guid,
		})
	}

	if fw.RebootRequired {
		out.Custom = append(out.Custom, types.Custom{
			Key:   "LVFS::DeviceFlags",
			Value: "skips-restart",
		})
		rebootMessage, err := dellGetString(fw.ImportantInfo, "en")
		if err != nil {
			return nil, err
		}

		out.Custom = append(out.Custom, types.Custom{
			Key:   "LVFS::UpdateMessage",
			Value: rebootMessage,
		})
	}

	summary, err := dellGetString(fw.Description, "en")
	if err != nil {
		return nil, err
	}
	out.Summary = summary
	out.Description = types.Description{
		Value: "<p>" + summary + "</p>",
	}

	out.MetadataLicense = "proprietary"
	out.ProjectLicense = "proprietary"

	out.Releases = append(out.Releases, types.Release{
		Version: fw.VendorVersion,
		Date:    fw.DateTime.String(),
		Description: types.Description{
			Value: "<p>" + summary + "</p>",
		},
		Urgency: dellGetUrgency(fw.Criticality.Value),
		Checksum: types.Checksum{
			Filename: path.Base(fw.Path),
			Target:   "content",
		},
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
	default:
		return nil, fmt.Errorf("no category matching for %s", fw.LUCategory.Value)
	}

	out.Custom = append(out.Custom, types.Custom{
		Key:   "LVFS::UpdateProtocol",
		Value: "org.dmtf.redfish",
	}, types.Custom{
		Key: "LVFS::DeviceIntegrity",
		// All Dell firmware going through Redfish are signed
		Value: "signed",
	})

	return &out, nil
}
