package types

type Vendor interface {
	// FetchCatalog retrieves the catalog of firmware for the vendor.
	FetchCatalog() (Catalog, error)
	// RetrieveFirmware downloads the firmware file for the given firmware entry.
	// For vendors like HPE, this step is required before processing.
	RetrieveFirmware(entry FirmwareEntry, tmpDir string) (string, error)
}

// Catalog represents a generic catalog of firmware entries.
type Catalog interface {
	// ListEntries returns all firmware entries in the catalog.
	ListEntries() []FirmwareEntry
}

// FirmwareEntry represents a single firmware entry in the catalog.
type FirmwareEntry interface {
	// GetFilename will be used to determine if the firmware has already been downloaded
	// and if it should be processed
	GetFilename() string
	// ToAppstream converts this firmware entry to an AppStream component.
	ToAppstream() (*Component, error)
}
