package lvfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaVersionPath(t *testing.T) {
	assert.Equal(t, "testdata/golden.xml.schema_version", schemaVersionPath("testdata/golden.xml"))
}

func TestWriteAndReadSchemaVersion(t *testing.T) {
	tmpDir := t.TempDir()
	goldenPath := filepath.Join(tmpDir, "golden.xml")

	err := writeSchemaVersion(goldenPath)
	require.NoError(t, err)

	v, err := readSchemaVersion(goldenPath)
	require.NoError(t, err)
	assert.Equal(t, MetadataSchemaVersion, v)
}

func TestReadSchemaVersion_MissingCompanion(t *testing.T) {
	tmpDir := t.TempDir()
	goldenPath := filepath.Join(tmpDir, "golden.xml")

	v, err := readSchemaVersion(goldenPath)
	require.NoError(t, err)
	assert.Equal(t, -1, v, "missing companion file should return -1 (legacy)")
}

func TestReadSchemaVersion_InvalidContent(t *testing.T) {
	tmpDir := t.TempDir()
	goldenPath := filepath.Join(tmpDir, "golden.xml")

	err := os.WriteFile(schemaVersionPath(goldenPath), []byte("not-a-number"), 0644)
	require.NoError(t, err)

	_, err = readSchemaVersion(goldenPath)
	assert.Error(t, err, "should error on invalid schema version content")
}

func TestAssertGolden_GoldenUpdateWritesCompanion(t *testing.T) {
	tmpDir := t.TempDir()
	goldenPath := filepath.Join(tmpDir, "golden.xml")
	content := []byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<component/>")

	t.Setenv("GO_GOLDEN_UPDATE", "1")
	assertGolden(t, goldenPath, content)

	got, err := os.ReadFile(goldenPath)
	require.NoError(t, err)
	assert.Equal(t, string(content), string(got))

	v, err := readSchemaVersion(goldenPath)
	require.NoError(t, err)
	assert.Equal(t, MetadataSchemaVersion, v, "companion file should store current MetadataSchemaVersion")
}

func TestCheckGoldenVersionEnforcement_HappyPath(t *testing.T) {
	content := []byte("<component/>")
	err := checkGoldenVersionEnforcement("golden.xml", content, content, MetadataSchemaVersion)
	assert.NoError(t, err, "should pass when golden matches and version matches")
}

func TestCheckGoldenVersionEnforcement_GoldenMismatchWithoutVersionBump(t *testing.T) {
	err := checkGoldenVersionEnforcement("golden.xml", []byte("<new/>"), []byte("<old/>"), MetadataSchemaVersion)
	assert.Error(t, err, "should fail when golden mismatches and version was not bumped")
	assert.Contains(t, err.Error(), "was not bumped")
}

func TestCheckGoldenVersionEnforcement_VersionBumpedButGoldenUnchanged(t *testing.T) {
	staleVersion := MetadataSchemaVersion - 1
	if staleVersion < 0 {
		staleVersion = 0
	}
	content := []byte("<same/>")
	err := checkGoldenVersionEnforcement("golden.xml", content, content, staleVersion)
	assert.Error(t, err, "should fail when version was bumped but golden output is unchanged")
	assert.Contains(t, err.Error(), "unchanged")
}

func TestCheckGoldenVersionEnforcement_BothChanged(t *testing.T) {
	staleVersion := MetadataSchemaVersion - 1
	if staleVersion < 0 {
		staleVersion = 0
	}
	err := checkGoldenVersionEnforcement("golden.xml", []byte("<new/>"), []byte("<old/>"), staleVersion)
	assert.Error(t, err, "should fail when golden mismatches (golden is stale even though version was bumped)")
	assert.Contains(t, err.Error(), "schema version was bumped")
}

func TestCheckGoldenVersionEnforcement_LegacyGoldenWithoutCompanion(t *testing.T) {
	content := []byte("<component/>")
	// storedVersion = -1 means legacy golden without .schema_version companion
	err := checkGoldenVersionEnforcement("golden.xml", content, content, -1)
	assert.Error(t, err, "should fail on legacy golden (version mismatch: -1 != current)")
}

func TestCheckGoldenVersionEnforcement_LegacyGoldenMismatch(t *testing.T) {
	// storedVersion = -1 means legacy golden; if output also differs, both differ
	err := checkGoldenVersionEnforcement("golden.xml", []byte("<new/>"), []byte("<old/>"), -1)
	assert.Error(t, err, "should fail when both golden and version differ (legacy golden)")
}
