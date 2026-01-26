# Firmirror

A firmware mirroring tool that creates LVFS-compatible repositories by fetching and converting firmware from hardware vendors (Dell and HPE).

## Overview

Firmirror automates the process of:
- Fetching firmware catalogs from vendor sources
- Downloading firmware packages
- Converting vendor-specific formats to LVFS/fwupd AppStream metadata
- Building CAB packages compatible with fwupd
- Maintaining a LVFS-compatible metadata index

## Features

- **Multi-vendor Support**: Currently supports Dell DSU and HPE SDR repositories
- **Incremental Processing**: Tracks processed firmware to avoid re-downloading
- **Pluggable Storage**: Abstract storage interface supporting local filesystem, with ability to add cloud storage (S3, GCS, etc.)
- **Metadata Signing**: Support for signing LVFS metadata using JCAT format with X.509 certificates

## Installation

### Prerequisites

- Go 1.19 or higher
- `fwupdtool` command-line tool
- `jcat-tool` for signature

### Building

```bash
go build -o firmirror ./cmd/firmirror.go
```

### Docker

A multi-stage Dockerfile is available:

```bash
docker build -t firmirror .
docker run -v /output:/output firmirror refresh /output --dell.enable
```

## Configuration

### CLI Flags

```
Global Flags:
  --help                Show help

Refresh Command:
  <out-dir>             Output directory for firmware and metadata

Dell Flags:
  --dell.enable         Enable Dell firmware mirroring
  --dell.machines-id    Comma-separated list of System IDs (e.g., 0C60,0C61)

HPE Flags:
  --hpe.enable          Enable HPE firmware mirroring
  --hpe.gens            Comma-separated list of generations (e.g., gen10,gen11)

Signature Flags:
  --sign.certificate    Path to certificate file for signing metadata (.pem or .crt)
  --sign.private-key    Path to private key file for signing metadata (.pem or .key)
```

## Usage

### Basic Command

```bash
# Mirror Dell firmware for specific machine types
./firmirror refresh /output/dir \
  --dell.enable \
  --dell.machines-id=0C60,0C61

# Mirror HPE firmware for specific generations
./firmirror refresh /output/dir \
  --hpe.enable \
  --hpe.gens=gen10,gen11

# Mirror both vendors
./firmirror refresh /output/dir \
  --dell.enable \
  --dell.machines-id=0C60 \
  --hpe.enable \
  --hpe.gens=gen10
```

### Output Structure

```
/output/dir/
├── firmware1.bin.cab       # CAB packages
├── firmware2.bin.cab
├── ...
├── metadata.xml.zst        # Compressed LVFS metadata
├── metadata.xml.zst.jcat   # JCAT signature file
└── metadata.xml            # Uncompressed metadata (temporary)
```

## Metadata Signing

Firmirror supports signing the LVFS metadata using the JCAT (JSON Catalog) format, which is compatible with fwupd's signature verification.

### How It Works

1. **JCAT File Creation**: After compressing the metadata (metadata.xml.zst), a corresponding .jcat file is created
2. **Checksums**: The JCAT file always includes SHA256 checksums for integrity verification
3. **Digital Signature**: If certificate and private key are provided, the metadata is signed using PKCS#7 format
4. **Storage**: Both the compressed metadata and its .jcat signature file are stored together

### Certificate Requirements

- X.509 certificate in PEM or CRT format
- Private key in PEM or KEY format
- The certificate should be trusted by the systems that will verify the metadata

### Example: Creating a Self-Signed Certificate

```bash
# Generate a private key
contrib/makecert.sh

# Use with firmirror
./firmirror refresh /output/dir \
  --dell.enable \
  --sign.certificate=cert.pem \
  --sign.private-key=key.pem
```

For production use, obtain certificates from a trusted Certificate Authority.
