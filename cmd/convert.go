package main

import (
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"path"

	"github.com/alecthomas/kong"

	"github.com/criteo/firmirror/cli"
	"github.com/criteo/firmirror/types"
	"github.com/criteo/firmirror/utils"
	"github.com/criteo/firmirror/vendors/dell"
	"github.com/criteo/firmirror/vendors/hpe"
)

func buildPackage(tmpDir string, appstream *types.Component) error {
	outBytes := []byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
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
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error building package: %s, %s", err, out)
	}

	return nil
}

const FILENAME = "16_35_4030-MCX562A-ACA_Ax_Bx.pldm.fwpkg"

func main() {
	ctx := kong.Parse(&cli.CLI)
	switch ctx.Command() {
	case "refresh <out-dir>":

	default:
		panic(ctx.Command())
	}

	tmpDir, err := os.MkdirTemp(".", "firmirror-*")
	if err != nil {
		fmt.Println(err)
		return
	}
	err = utils.CopyFile(FILENAME, path.Join(tmpDir, FILENAME))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(tmpDir)
	os.Remove(FILENAME + ".cab")

	appstream, err := hpe.HandleHPEFirmware(FILENAME)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = buildPackage(tmpDir, appstream)
	if err != nil {
		fmt.Println(err)
		return
	}

	dellCatalog, err := dell.DellFetchCatalog()
	if err != nil {
		fmt.Println(err)
		return
	}

	dellSystems := map[string]bool{
		"0C60": true,
	}
	toFetch := []dell.DellSoftwareComponent{}
	for _, swComponent := range dellCatalog.SoftwareComponents {
		// Only select firmware, not drivers
		if swComponent.ComponentType.Value != "FRMW" {
			continue
		}
		for _, brands := range swComponent.SupportedSystems {
			for _, system := range brands.Models {
				if dellSystems[system.SystemID] {
					toFetch = append(toFetch, swComponent)
				}
			}
		}
	}

	// for _, fw := range toFetch {
	tmpDir, err = os.MkdirTemp(".", "firmirror-*")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(tmpDir)
	dell.DellDownloadFirmware(toFetch[44], tmpDir)
	appstream, err = dell.HandleDellFirmware(toFetch[44])
	if err != nil {
		fmt.Println(err)
		return
	}

	err = buildPackage(tmpDir, appstream)
	if err != nil {
		fmt.Println(err)
		return
	}
	// }
}
