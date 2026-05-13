package lvfs

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// schemaVersionPath returns the companion .schema_version file path for a golden file.
func schemaVersionPath(goldenPath string) string {
	return goldenPath + ".schema_version"
}

// writeSchemaVersion stores the current MetadataSchemaVersion alongside the golden file.
func writeSchemaVersion(goldenPath string) error {
	return os.WriteFile(schemaVersionPath(goldenPath), []byte(strconv.Itoa(MetadataSchemaVersion)), 0644)
}

// readSchemaVersion reads the stored schema version from the companion file.
// Returns -1 if the file does not exist (legacy golden without enforcement).
func readSchemaVersion(goldenPath string) (int, error) {
	data, err := os.ReadFile(schemaVersionPath(goldenPath))
	if err != nil {
		if os.IsNotExist(err) {
			return -1, nil
		}
		return 0, err
	}
	v, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, fmt.Errorf("parsing schema version from %s: %w", schemaVersionPath(goldenPath), err)
	}
	return v, nil
}

func AssertGoldenComponents(t *testing.T, goldenPath string, components []Component) {
	t.Helper()

	var out []byte
	for _, comp := range components {
		out = append(out, []byte(xml.Header)...)
		xmlBytes, err := xml.MarshalIndent(comp, "", "  ")
		if err != nil {
			t.Fatalf("marshaling component %s: %v", comp.ID, err)
		}
		out = append(out, xmlBytes...)
		out = append(out, '\n')
	}

	assertGolden(t, goldenPath, out)
}

func AssertGoldenComponentsXML(t *testing.T, goldenPath string, components *Components) {
	t.Helper()

	out := []byte(xml.Header)
	xmlBytes, err := xml.MarshalIndent(components, "", "  ")
	if err != nil {
		t.Fatalf("marshaling components: %v", err)
	}
	out = append(out, xmlBytes...)

	assertGolden(t, goldenPath, out)
}

// checkGoldenVersionEnforcement validates that golden output changes are
// accompanied by MetadataSchemaVersion bumps, and vice versa.
// Returns nil on the happy path, or an error describing the enforcement failure.
func checkGoldenVersionEnforcement(goldenPath string, got, expected []byte, storedVersion int) error {
	goldenMatches := string(got) == string(expected)
	versionMatches := storedVersion == MetadataSchemaVersion

	switch {
	case goldenMatches && versionMatches:
		return nil

	case !goldenMatches && !versionMatches:
		// Golden changed AND version was bumped — this is the expected flow.
		// The golden is now stale; developer must run GO_GOLDEN_UPDATE=1 to regenerate.
		return fmt.Errorf("Golden file mismatch for %s (schema version was bumped from %d to %d).\n\nRun: GO_GOLDEN_UPDATE=1 go test ./... -run TestGolden\n\ngot:\n%s\nexpected:\n%s",
			goldenPath, storedVersion, MetadataSchemaVersion, got, expected)

	case !goldenMatches && versionMatches:
		// Golden changed but version was NOT bumped — enforcement failure.
		return fmt.Errorf("Golden file mismatch for %s but MetadataSchemaVersion was not bumped!\n\nYou must:\n  1. Bump lvfs.MetadataSchemaVersion in pkg/lvfs/lvfs.go\n  2. Run: GO_GOLDEN_UPDATE=1 go test ./... -run TestGolden\n\ngot:\n%s\nexpected:\n%s",
			goldenPath, got, expected)

	case goldenMatches && !versionMatches:
		// Version was bumped but golden output didn't change.
		// This means the version bump is unnecessary (no processing logic change).
		return fmt.Errorf("MetadataSchemaVersion was bumped from %d to %d but golden output for %s is unchanged.\n\nIf no processing logic changed, revert the version bump.\nIf processing logic did change, the golden output should differ — investigate.",
			storedVersion, MetadataSchemaVersion, goldenPath)
	}

	return nil
}

func assertGolden(t *testing.T, goldenPath string, got []byte) {
	t.Helper()

	if os.Getenv("GO_GOLDEN_UPDATE") == "1" {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0755); err != nil {
			t.Fatalf("creating testdata dir: %v", err)
		}
		if err := os.WriteFile(goldenPath, got, 0644); err != nil {
			t.Fatalf("writing golden file %s: %v", goldenPath, err)
		}
		if err := writeSchemaVersion(goldenPath); err != nil {
			t.Fatalf("writing schema version for %s: %v", goldenPath, err)
		}
		t.Logf("Updated golden file at %s (schema_version=%d)", goldenPath, MetadataSchemaVersion)
		return
	}

	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("reading golden file %s: %v\nRun with GO_GOLDEN_UPDATE=1 to generate it.\nAlso bump lvfs.MetadataSchemaVersion if the change is intentional.", goldenPath, err)
	}

	storedVersion, err := readSchemaVersion(goldenPath)
	if err != nil {
		t.Fatalf("reading schema version for %s: %v", goldenPath, err)
	}

	if err := checkGoldenVersionEnforcement(goldenPath, got, expected, storedVersion); err != nil {
		t.Error(err)
	}
}
