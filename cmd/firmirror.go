package main

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kong"

	"github.com/criteo/firmirror/pkg/firmirror"
	"github.com/criteo/firmirror/pkg/vendors/dell"
	"github.com/criteo/firmirror/pkg/vendors/hpe"
)

type DellFlags struct {
	Enable     bool     `help:"Enable Dell firmware fetching." default:"false"`
	MachinesID []string `help:"List of machine IDs to fetch firmware for. They are composed of 4 characters representing the machine type, followed by 4 digits representing the hexadecimal machine ID. For example: \"0C60\" for \"3168\" corresponding to the C6615 series of servers. You can also specify \"*\" to fetch all the firmware, but this may take a very long time."`
}

type HPEFlags struct {
	Enable bool     `help:"Enable HPE firmware fetching." default:"false"`
	Gens   []string `help:"List of generations to fetch firmware for." default:"gen10,gen11,gen12" enum:"gen10,gen11,gen12"`
}

type S3 struct {
	Enable   bool   `help:"Use S3 storage backend instead of local filesystem. Requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables" default:"false"`
	Bucket   string `help:"S3 bucket name for storing firmware files"`
	Prefix   string `help:"Optional prefix for all S3 keys" default:""`
	Region   string `help:"AWS region" default:"us-east-1"`
	Endpoint string `help:"Custom S3 endpoint URL (for S3-compatible services like MinIO)" default:""`
}

type Signature struct {
	Certificate string `help:"Path to certificate file for signing metadata (.pem or .crt)" type:"path"`
	PrivateKey  string `help:"Path to private key file for signing metadata (.pem or .key)" type:"path"`
}

var args struct {
	DellFlags `embed:"" prefix:"dell." group:"Dell" help:"Dell firmware fetching."`
	HPEFlags  `embed:"" prefix:"hpe." group:"HPE" help:"HPE firmware fetching."`
	S3        `embed:"" prefix:"s3." group:"S3 Storage" help:"S3 storage backend configuration."`
	Signature `embed:"" prefix:"sign." group:"Signature" help:"Metadata signing configuration."`
	OutputDir string `help:"Output directory for the LVFS-compatible firmware repository (ignored when using S3)" type:"path"`
	Refresh   struct {
	} `cmd:"" help:"Refresh all the firmware from the repositories. Note: this will not replace the already-existing firmware, even if the vendor pushed an updated version. You will need to delete the firmware manually."`
}

func main() {
	cli := kong.Parse(&args)
	switch cli.Command() {
	case "refresh":
	default:
		panic(cli.Command())
	}

	// Check if bin tools are available
	for _, bin := range []string{"fwupdtool", "jcat-tool"} {
		if _, err := exec.LookPath(bin); err != nil {
			slog.Error(bin + " is required but not found in PATH, aborting")
			return
		}
	}

	// Create storage backend
	var storage firmirror.Storage
	var err error

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	if args.S3.Enable {
		storage, err = firmirror.NewS3Storage(context.Background(), args.S3.Bucket, args.S3.Prefix, args.S3.Region, args.S3.Endpoint)
		if err != nil {
			slog.Error("Failed to create S3 storage backend", "error", err)
			return
		}
		slog.Info("Using S3 storage backend", "bucket", args.S3.Bucket, "prefix", args.S3.Prefix)
	} else {
		if args.OutputDir == "" {
			slog.Error("Output directory is required when using local storage")
			return
		}

		storage, err = firmirror.NewLocalStorage(args.OutputDir)
		if err != nil {
			slog.Error("Failed to create local storage backend", "error", err)
			return
		}
		slog.Info("Using local filesystem storage", "path", args.OutputDir)
	}

	config := firmirror.FirmirrorConfig{
		CacheDir:    ".firmirror_cache",
		Certificate: args.Signature.Certificate,
		PrivateKey:  args.Signature.PrivateKey,
	}

	if !args.HPEFlags.Enable && !args.DellFlags.Enable {
		slog.Error("No vendor enabled, exiting")
		return
	}

	fm := firmirror.NewFirmirrorSyncer(config, storage)

	if args.HPEFlags.Enable {
		for _, gen := range args.HPEFlags.Gens {
			hpeRepo := "fwpp-" + gen
			hpeVendor := hpe.NewHPEVendor(hpeRepo)
			fm.RegisterVendor("hpe-"+gen, hpeVendor)
		}
	}

	if args.DellFlags.Enable {
		dellVendor := dell.NewDellVendor(args.DellFlags.MachinesID)
		fm.RegisterVendor("dell", dellVendor)
	}

	defer func() {
		slog.Info("Saving repository metadata")
		if err := fm.SaveMetadata(context.Background()); err != nil {
			slog.Error("Failed to save metadata", "error", err)
		}

		stop()
	}()

	// Load existing metadata to avoid reprocessing
	if err := fm.LoadMetadata(ctx); err != nil {
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
