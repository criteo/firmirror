package hpe

// HPEVendor implements the Vendor interface for HPE
type HPEVendor struct {
	BaseURL string
}

// HPEVendor implements the Catalog interface for HPE
type HPECatalog struct {
	Entries map[string]HPECatalogEntry `json:",inline"`
}

// HPEFirmwareEntry implements the FirmwareEntry interface for HPE
type HPEFirmwareEntry struct {
	Filename     string
	Entry        *HPECatalogEntry
	downloadPath string // Store download path for processing
}

type HPECatalogEntry struct {
	Date                 string   `json:"date"`
	Description          string   `json:"description"`
	DeviceClass          string   `json:"deviceclass"`
	MinimumActiveVersion string   `json:"minimum_active_version"`
	RebootRequired       string   `json:"reboot_required"`
	ServerPowerOff       string   `json:"server_power_off"`
	Target               []string `json:"target"`
	Version              string   `json:"version"`
}

type HPEPayload struct {
	DeviceClass   string     `json:"DeviceClass"`
	Devices       HPEDevices `json:"Devices"`
	PackageFormat string     `json:"PackageFormat"`
	Type          string     `json:"Type"`
	UpdatableBy   []string   `json:"UpdatableBy"`
	Package       HPEPackage `json:"package"`
}

type HPEDevices struct {
	Device []HPEDevice `json:"Device"`
}

type HPEDevice struct {
	DeviceName     string             `json:"DeviceName"`
	FirmwareImages []HPEFirmwareImage `json:"FirmwareImages"`
	Target         string             `json:"Target"`
	Version        string             `json:"Version"`
}

type HPEFirmwareImage struct {
	DelayAfterInstallSec int    `json:"DelayAfterInstallSec"`
	DirectFlashOk        bool   `json:"DirectFlashOk"`
	FileName             string `json:"FileName"`
	InstallDurationSec   int    `json:"InstallDurationSec"`
	Order                int    `json:"Order"`
	PLDMImage            bool   `json:"PLDMImage"`
	ResetRequired        bool   `json:"ResetRequired"`
	ServerPowerOff       bool   `json:"ServerPowerOff"`
	SysPowerON           bool   `json:"SysPowerON"`
	Type                 string `json:"Type"`
	UefiFlashable        bool   `json:"UefiFlashable"`
}

type HPEPackage struct {
	Category               []HPECategory             `json:"category"`
	Description            []HPETranslations         `json:"description"`
	Divisions              []HPEDivision             `json:"divisions"`
	Files                  map[string]any            `json:"files"`
	ID                     HPEID                     `json:"id"`
	Installation           HPEInstallation           `json:"installation"`
	InstallationDependency HPEInstallationDependency `json:"installation_dependency"`
	ManufacturerName       []HPETranslations         `json:"manufacturer_name"`
	Name                   []HPETranslations         `json:"name"`
	Prerequisites          HPEPrerequisites          `json:"prerequisites"`
	ReleaseDate            string                    `json:"release_date"`
	SchemaVersion          string                    `json:"schema_version"`
	SupportedProducts      []HPESupportedProduct     `json:"supported_products"`
	SwKeys                 []HPESwKey                `json:"sw_keys"`
}

type HPECategory struct {
	Key       string            `json:"key"`
	Languages []HPETranslations `json:"languages"`
}

type HPEDivision struct {
	Key      string            `json:"key"`
	Language []HPETranslations `json:"language"`
}

type HPEID struct {
	Product string `json:"product"`
	Version string `json:"version"`
}

type HPEInstallation struct {
	Command                 string            `json:"command"`
	CommandParams           string            `json:"command_params"`
	InstallCaps             HPEInstallCaps    `json:"install_caps"`
	PerDeviceInstallTimeSec int               `json:"per_device_install_time_seconds"`
	RebootDetails           []HPERebootDetail `json:"reboot_details"`
	RebootRequired          string            `json:"reboot_required"`
}

type HPEInstallCaps struct {
	NeedUserAcct string `json:"needuseracct"`
	Silent       string `json:"silent"`
}

type HPERebootDetail struct {
	Language []HPETranslations `json:"language"`
}

type HPEInstallationDependency struct {
	DependencyRequirement HPEDependencyRequirement `json:"DependencyRequirement"`
}

type HPEDependencyRequirement struct {
	ApplicableTo HPEApplicableTo `json:"ApplicableTo"`
	DateCreated  string          `json:"DateCreated"`
	Requires     HPEResquires    `json:"Requires"`
}

type HPEApplicableTo struct {
	GuidTargets HPEGuidTargets `json:"GuidTargets"`
}

type HPEGuidTargets struct {
	GuidTarget []string `json:"GuidTarget"`
}

type HPEResquires struct {
	Requirements []string `json:"Requirements"`
}

type HPETranslations struct {
	Lang  string `json:"lang"`
	XLate string `json:"x_late"`
}

type HPEPrerequisites struct {
	RequiredDiskSpace         HPERequiredDiskSpace          `json:"required_diskspace"`
	SupportedDevices          []HPESupportedDevice          `json:"supported_devices"`
	SupportedOperatingSystems []HPESupportedOperatingSystem `json:"supported_operating_systems"`
	SupportedPlatforms        bool                          `json:"supported_platforms"`
}

type HPERequiredDiskSpace struct {
	SizeKB string `json:"size_kb"`
}

type HPESupportedDevice struct {
	Dev        string `json:"dev"`
	SubDev     string `json:"subdev"`
	SubVen     string `json:"subven"`
	TargetGuid string `json:"target_guid"`
	Type       string `json:"type"`
	Ven        string `json:"ven"`
}

type HPESupportedOperatingSystem struct {
	Major        string `json:"major"`
	Minor        string `json:"minor"`
	Name         string `json:"name"`
	Platform     string `json:"platform"`
	SR           string `json:"sr"`
	MinimumBuild string `json:"minimumbuild,omitempty"`
	MaximumBuild string `json:"maximumbuild,omitempty"`
}

type HPESupportedProduct struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type HPESwKey struct {
	Name              string `json:"name"`
	SwKeyExpectedPath string `json:"sw_key_expectedpath"`
}
