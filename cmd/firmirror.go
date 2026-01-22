package main

import (
	"log/slog"

	"github.com/alecthomas/kong"

	"github.com/criteo/firmirror/pkg/firmirror"
	"github.com/criteo/firmirror/pkg/vendors/dell"
	"github.com/criteo/firmirror/pkg/vendors/hpe"
)

func main() {
	ctx := kong.Parse(&firmirror.CLI)
	switch ctx.Command() {
	case "refresh <out-dir>":
	default:
		panic(ctx.Command())
	}

	fmConf := firmirror.FirmirrorConfig{
		OutputDir: firmirror.CLI.Refresh.OutDir,
		CacheDir:  ".firmirror_cache",
	}

	if !firmirror.CLI.HPEFlags.Enable && !firmirror.CLI.DellFlags.Enable {
		slog.Error("No vendor enabled, exiting")
		return
	}

	fm := firmirror.NewFimirrorSyncer(fmConf)

	if firmirror.CLI.HPEFlags.Enable {
		for _, gen := range firmirror.CLI.HPEFlags.Gens {
			hpeRepo := "fwpp-" + gen
			hpeVendor := hpe.NewHPEVendor(hpeRepo)
			fm.RegisterVendor("hpe-"+gen, hpeVendor)
		}
	}

	if firmirror.CLI.DellFlags.Enable {
		dellVendor := dell.NewDellVendor(firmirror.CLI.DellFlags.MachinesID)
		fm.RegisterVendor("dell", dellVendor)
	}

	slog.Info("Starting firmware processing", "vendors", len(fm.GetAllVendors()))

	for vendorName, vendor := range fm.GetAllVendors() {
		slog.Info("Processing vendor", "name", vendorName)
		if err := fm.ProcessVendor(vendor, vendorName); err != nil {
			slog.Error("Failed to process vendor", "vendor", vendorName, "error", err)
		}
	}

	slog.Info("Firmware processing completed")
}
