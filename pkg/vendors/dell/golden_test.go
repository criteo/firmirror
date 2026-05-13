package dell

import (
	"bytes"
	"encoding/xml"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/premday/firmirror/pkg/lvfs"
)

func TestGoldenAppStream(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "catalog.xml"))
	if err != nil {
		t.Fatalf("reading catalog.xml: %v", err)
	}

	var catalog DellCatalog
	decoder := xml.NewDecoder(bytes.NewReader(data))
	decoder.CharsetReader = func(charset string, input io.Reader) (io.Reader, error) {
		return input, nil
	}
	if err := decoder.Decode(&catalog); err != nil {
		t.Fatalf("parsing catalog: %v", err)
	}

	filtered := (&DellVendor{}).filterCatalog(&catalog)
	if len(filtered.SoftwareComponents) == 0 {
		t.Fatal("no FRMW entries in catalog")
	}

	fw := filtered.SoftwareComponents[0]
	entry := &DellFirmwareEntry{
		Filename:              filepath.Base(fw.Path),
		DellSoftwareComponent: &fw,
	}

	components, err := entry.ToAppstream()
	if err != nil {
		t.Fatalf("ToAppstream failed: %v", err)
	}

	goldenPath := filepath.Join("testdata", "golden.xml")
	lvfs.AssertGoldenComponents(t, goldenPath, components)
}
