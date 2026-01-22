package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kong"

	"github.com/criteo/firmirror/pkg/firmirror"
	"github.com/criteo/firmirror/pkg/vendors/dell"
	"github.com/criteo/firmirror/pkg/vendors/hpe"
)

func main() {
	cli := kong.Parse(&firmirror.CLI)
	switch cli.Command() {
	case "refresh <out-dir>":
	default:
		panic(cli.Command())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// Create storage backend (default to local filesystem)
	storage, err := firmirror.NewLocalStorage(firmirror.CLI.Refresh.OutDir)
	if err != nil {
		slog.Error("Failed to create storage backend", "error", err)
		return
	}

	config := firmirror.FirmirrorConfig{
		CacheDir: ".firmirror_cache",
	}

	if !firmirror.CLI.HPEFlags.Enable && !firmirror.CLI.DellFlags.Enable {
		slog.Error("No vendor enabled, exiting")
		return
	}

	fm := firmirror.NewFimirrorSyncer(config, storage)

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

	defer func() {
		slog.Info("Saving repository metadata")
		if err := fm.SaveMetadata(); err != nil {
			slog.Error("Failed to save metadata", "error", err)
		}

		stop()
	}()

	// Load existing metadata to avoid reprocessing
	if err := fm.LoadMetadata(); err != nil {
		slog.Error("Failed to load existing metadata", "error", err)
	}

	slog.Info("Starting firmware processing", "vendors", len(fm.GetAllVendors()))

	for vendorName, vendor := range fm.GetAllVendors() {
		if ctx.Err() != nil {
			slog.Info("Shutdown requested, stopping processing")
			break
		}

		slog.Info("Processing vendor", "name", vendorName)
		if err := fm.ProcessVendor(ctx, vendor, vendorName); err != nil && err != context.Canceled {
			slog.Error("Failed to process vendor", "vendor", vendorName, "error", err)
		}
	}
}
