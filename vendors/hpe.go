package vendors

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/criteo/firmirror/types"
	"github.com/criteo/firmirror/utils"
)

func hpeGetString(strings []types.HPETranslations, language string) (string, error) {
	for _, l := range strings {
		if l.Lang == language {
			return l.XLate, nil
		}
	}
	return "", fmt.Errorf("language not found: %s", language)
}

func HandleHPEFirmware(filename string) (*types.Component, error) {
	r, err := zip.OpenReader(filename)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	payloadFile, err := utils.GetFileFromName("payload.json", r)
	if err != nil {
		return nil, err
	}

	reader, err := payloadFile.Open()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer reader.Close()

	byteValue, _ := io.ReadAll(reader)

	var payload types.HPEPayload

	err = json.Unmarshal(byteValue, &payload)
	if err != nil {
		return nil, err
	}

	appstream, err := hpeFWToAppStream(payload)
	if err != nil {
		return nil, err
	}

	appstream.Releases[0].Checksum = types.Checksum{
		Filename: filename,
		Target:   "content",
	}

	return appstream, nil
}

// hpeFWToAppStream converts an HPE firmware payload to an AppStream component.
// Note: we make the assumption that all devices in the payload will have the same version
// as well as the install duration.
func hpeFWToAppStream(fw types.HPEPayload) (*types.Component, error) {
	out := types.Component{}

	out.Type = "firmware"

	// TODO:

	var devices []string
	var guids []string
	for _, dev := range fw.Devices.Device {
		devices = append(devices, dev.DeviceName)
		// TODO: properly create GUID
		guids = append(guids, dev.Target)
	}
	slices.Sort(devices)
	devices = slices.Compact(devices)

	for _, guid := range guids {
		out.Provides = append(out.Provides, types.Firmware{
			Type: "flashed",
			Text: guid,
		})
	}

	manufacturer, err := hpeGetString(fw.Package.ManufacturerName, "en")
	if err != nil {
		return nil, err
	}

	out.ID = fmt.Sprintf("com.%s.%s", strings.ToLower(strings.ReplaceAll(manufacturer, " ", "")), strings.ReplaceAll(fw.Package.SwKeys[0].Name, " ", ""))
	out.Name = strings.Join(devices[:], "/")

	out.DeveloperName = manufacturer

	if fw.Package.Installation.RebootRequired == "yes" {
		out.Custom = append(out.Custom, types.Custom{
			Key:   "LVFS::DeviceFlags",
			Value: "skips-restart",
		})
		rebootMessage, err := hpeGetString(fw.Package.Installation.RebootDetails[0].Language, "en")
		if err != nil {
			return nil, err
		}

		out.Custom = append(out.Custom, types.Custom{
			Key:   "LVFS::UpdateMessage",
			Value: rebootMessage,
		})
	}

	summary, err := hpeGetString(fw.Package.Name, "en")
	if err != nil {
		return nil, err
	}
	out.Summary = strings.ReplaceAll(strings.ReplaceAll(summary, "\t", ""), "  ", " ")

	description, err := hpeGetString(fw.Package.Description, "en")
	if err != nil {
		return nil, err
	}
	out.Description = types.Description{
		Value: "<p>" + description + "</p>",
	}

	out.MetadataLicense = "proprietary"
	out.ProjectLicense = "proprietary"

	releaseDate, err := time.Parse("2006-01-02T15:04:05", fw.Package.ReleaseDate)
	if err != nil {
		return nil, err
	}

	out.Releases = append(out.Releases, types.Release{
		Version:         fw.Devices.Device[0].Version,
		Date:            releaseDate.Format(time.DateOnly),
		InstallDuration: fw.Devices.Device[0].FirmwareImages[0].InstallDurationSec,
		Description: types.Description{
			Value: "<p>" + description + "</p>",
		},
	})

	for _, category := range fw.Package.Category {
		switch category.Key {
		case "2900095":
			// Firmware - Network
			out.Categories = append(out.Categories, "X-NetworkInterface")
		case "2900213":
			// Firmware - iLO
			out.Categories = append(out.Categories, "X-BaseboardManagementController")
		}
	}

	out.Custom = append(out.Custom, types.Custom{
		Key:   "LVFS::UpdateProtocol",
		Value: "org.dmtf.redfish",
	}, types.Custom{
		Key: "LVFS::DeviceIntegrity",
		// All fwpkg going through Redfish are signed
		Value: "signed",
	})

	return &out, nil
}
