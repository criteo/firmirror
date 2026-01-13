package firmirror

type DellFlags struct {
	Enable     bool     `help:"Enable Dell firmware fetching." default:"false"`
	MachinesID []string `help:"List of machine IDs to fetch firmware for. They are composed of 4 characters representing the machine type, followed by 4 digits representing the hexadecimal machine ID. For example: \"0C60\" for \"3168\" corresponding to the C6615 series of servers. You can also specify \"*\" to fetch all the firmware, but this may take a very long time."`
}

type HPEFlags struct {
	Enable bool     `help:"Enable HPE firmware fetching." default:"false"`
	Gens   []string `help:"List of generations to fetch firmware for." default:"gen10,gen11,gen12" enum:"gen10,gen11,gen12"`
}

type CLIFlags struct {
	DellFlags `embed:"" prefix:"dell." group:"Dell" help:"Dell firmware fetching."`
	HPEFlags  `embed:"" prefix:"hpe." group:"HPE" help:"HPE firmware fetching."`
	Refresh   struct {
		OutDir string `arg:"" help:"Output directory for the LVFS-compatible firmware repository" type:"path"`
	} `cmd:"" help:"Refresh all the firmware from the repositories. Note: this will not replace the already-existing firmware, even if the vendor pushed an updated version. You will need to delete the firmware manually."`
}

var CLI CLIFlags
