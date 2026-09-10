package filedb_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/davidcassany/ocistore/pkg/filedb"
)

type entry struct {
	digest    string
	relPaths  []string
	temporary bool
}

func (e entry) Digest() string {
	return e.digest
}

func (e entry) RelPaths() []string {
	return e.relPaths
}

func (e entry) IsTemporary() bool {
	return e.temporary
}

func openTemp(t *testing.T) *filedb.DB {
	t.Helper()
	db, err := filedb.Open(filepath.Join(t.TempDir(), "filedb.bolt"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestPathsForChecksum_unknown(t *testing.T) {
	db := openTemp(t)
	paths, err := db.PathsForChecksum("sha256:doesnotexist")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Fatalf("expected empty slice, got %v", paths)
	}
}

func TestRecordAll_and_PathsForChecksum(t *testing.T) {
	db := openTemp(t)

	root := "/extractions/root1"
	entries := []filedb.Entry{
		&entry{digest: "sha256:aaa", relPaths: []string{"usr/bin/foo", "usr/bin/foo2"}}, // Includes duplicates
		&entry{digest: "sha256:bbb", relPaths: []string{"usr/lib/bar.so"}},
		&entry{digest: "", relPaths: []string{"usr/share/doc"}}, // directory — no digest, must be skipped
	}

	if err := db.RecordAll(root, entries); err != nil {
		t.Fatalf("RecordAll: %v", err)
	}

	paths, err := db.PathsForChecksum("sha256:aaa")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(root, "usr/bin/foo"),
		filepath.Join(root, "usr/bin/foo2"),
	}
	slices.Sort(paths)
	slices.Sort(want)
	if !slices.Equal(paths, want) {
		t.Errorf("PathsForChecksum(aaa): got %v, want %v", paths, want)
	}

	// digest with no paths in the DB
	paths, err = db.PathsForChecksum("sha256:zzz")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Errorf("expected no paths for unknown digest, got %v", paths)
	}
}

func TestRemoveRoot_cleansUpOrphanDigests(t *testing.T) {
	db := openTemp(t)

	root1 := "/extractions/root1"
	root2 := "/extractions/root2"
	sharedDigest := "sha256:shared"
	onlyRoot1 := "sha256:only1"

	if err := db.RecordAll(root1, []filedb.Entry{
		&entry{digest: sharedDigest, relPaths: []string{"usr/bin/foo"}},
		&entry{digest: onlyRoot1, relPaths: []string{"usr/lib/private.so"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordAll(root2, []filedb.Entry{
		&entry{digest: sharedDigest, relPaths: []string{"usr/bin/foo"}},
	}); err != nil {
		t.Fatal(err)
	}

	// Before removal: sharedDigest has two paths.
	paths, _ := db.PathsForChecksum(sharedDigest)
	if len(paths) != 2 {
		t.Fatalf("expected 2 paths for sharedDigest before removal, got %v", paths)
	}

	if err := db.RemoveRoot(root1); err != nil {
		t.Fatalf("RemoveRoot: %v", err)
	}

	// sharedDigest still has root2's path.
	paths, err := db.PathsForChecksum(sharedDigest)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Join(root2, "usr/bin/foo")}
	if !slices.Equal(paths, want) {
		t.Errorf("PathsForChecksum(shared) after removing root1: got %v, want %v", paths, want)
	}

	// onlyRoot1 digest must be gone entirely.
	paths, err = db.PathsForChecksum(onlyRoot1)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Errorf("expected onlyRoot1 digest to be GC'd, got %v", paths)
	}
}

func TestRemoveRoot_nonexistentIsNoop(t *testing.T) {
	db := openTemp(t)
	if err := db.RemoveRoot("/does/not/exist"); err != nil {
		t.Fatalf("RemoveRoot on unknown root: %v", err)
	}
}

func TestRemoveRoot_allRoots_clearsAllDigests(t *testing.T) {
	db := openTemp(t)

	root1 := "/extractions/root1"
	root2 := "/extractions/root2"
	digest := "sha256:abc"

	db.RecordAll(root1, []filedb.Entry{&entry{digest: digest, relPaths: []string{"bin/x"}}})
	db.RecordAll(root2, []filedb.Entry{&entry{digest: digest, relPaths: []string{"bin/x"}}})

	db.RemoveRoot(root1)
	db.RemoveRoot(root2)

	paths, err := db.PathsForChecksum(digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Errorf("expected digest to be fully GC'd, got %v", paths)
	}
}

func sha256digest(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", h)
}

func TestScanRoot(t *testing.T) {
	dir := t.TempDir()

	files := map[string][]byte{
		"file1.txt":        []byte("hello world"),
		"subdir/file2.txt": []byte("nested content"),
	}
	for rel, data := range files {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// empty file and symlink must be skipped
	os.WriteFile(filepath.Join(dir, "empty.txt"), []byte{}, 0o644)
	os.Symlink("file1.txt", filepath.Join(dir, "link.txt"))

	entries, err := filedb.ScanRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(files) {
		t.Fatalf("expected %d entries, got %d", len(files), len(entries))
	}

	got := map[string]string{}
	for _, e := range entries {
		for _, rp := range e.RelPaths() {
			got[rp] = e.Digest()
		}
	}
	for rel, data := range files {
		want := sha256digest(data)
		if got[rel] != want {
			t.Errorf("%q: got digest %q, want %q", rel, got[rel], want)
		}
	}
}

func TestScanRoot_integratesWithStagedCommit(t *testing.T) {
	dir := t.TempDir()

	content := []byte("some file content")
	if err := os.MkdirAll(filepath.Join(dir, "usr/bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "usr/bin/tool"), content, 0o755); err != nil {
		t.Fatal(err)
	}

	entries, err := filedb.ScanRoot(dir)
	if err != nil {
		t.Fatalf("ScanRoot: %v", err)
	}

	db := openTemp(t)
	if err := db.RecordAll(dir, entries); err != nil {
		t.Fatalf("RecordAll: %v", err)
	}

	digest := sha256digest(content)
	paths, err := db.PathsForChecksum(digest)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Join(dir, "usr/bin/tool")}
	if !slices.Equal(paths, want) {
		t.Errorf("PathsForChecksum after ScanRoot+CommitRoot: got %v, want %v", paths, want)
	}
}

func TestRecordAllStaged_notVisibleInChecksums(t *testing.T) {
	db := openTemp(t)

	if err := db.RecordAllStaged("stage1", []filedb.Entry{
		&entry{digest: "sha256:aaa", relPaths: []string{"usr/bin/foo"}},
	}); err != nil {
		t.Fatalf("RecordAllStaged: %v", err)
	}

	if !db.StagingExists("stage1") {
		t.Error("expected staging to exist after RecordAllStaged")
	}
	paths, err := db.PathsForChecksum("sha256:aaa")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Errorf("staged entry must not appear in checksums before CommitRoot, got %v", paths)
	}
}

func TestRecordAllStaged_and_StagedPathsForChecksum(t *testing.T) {
	db := openTemp(t)

	if err := db.RecordAllStaged("stage1", []filedb.Entry{
		&entry{digest: "sha256:aaa", relPaths: []string{"usr/bin/foo"}},
	}); err != nil {
		t.Fatalf("RecordAllStaged: %v", err)
	}

	if err := db.RecordAllStaged("stage1", []filedb.Entry{
		&entry{digest: "sha256:aaa", relPaths: []string{"usr/bin/foo-copy"}},
	}); err != nil {
		t.Fatalf("RecordAllStaged: %v", err)
	}

	paths, err := db.StagedPathsForChecksum("stage1", "sha256:aaa")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Errorf("staged paths must be accessible by digest, got %v", paths)
	}
	if paths[0] != "usr/bin/foo" {
		t.Errorf("staged path is not matching, got %s", paths[0])
	}
	if paths[1] != "usr/bin/foo-copy" {
		t.Errorf("staged path is not matching, got %s", paths[1])
	}
}

func TestCommitRoot_updatesAllIndexes(t *testing.T) {
	db := openTemp(t)

	finalRoot := "/extractions/final"
	entries := []filedb.Entry{
		&entry{digest: "sha256:aaa", relPaths: []string{"usr/bin/foo", "usr/bin/foo2"}},
		&entry{digest: "sha256:bbb", relPaths: []string{"usr/lib/bar.so"}},
		&entry{digest: "", relPaths: []string{"usr/share/doc"}}, // directory — skipped
	}

	if err := db.RecordAllStaged("stage1", entries); err != nil {
		t.Fatalf("RecordAllStaged: %v", err)
	}
	if err := db.CommitRoot("stage1", finalRoot); err != nil {
		t.Fatalf("CommitRoot: %v", err)
	}

	if db.StagingExists("stage1") {
		t.Error("staging bucket must be removed after CommitRoot")
	}
	if !db.RootExists(finalRoot) {
		t.Error("final root must exist after CommitRoot")
	}

	paths, err := db.PathsForChecksum("sha256:aaa")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(finalRoot, "usr/bin/foo"),
		filepath.Join(finalRoot, "usr/bin/foo2"),
	}
	slices.Sort(paths)
	slices.Sort(want)
	if !slices.Equal(paths, want) {
		t.Errorf("PathsForChecksum(aaa) after CommitRoot: got %v, want %v", paths, want)
	}
}

func TestCommitRoot_stagingNotFoundReturnsError(t *testing.T) {
	db := openTemp(t)
	if err := db.CommitRoot("nonexistent", "/extractions/final"); err == nil {
		t.Fatal("expected error when staging ID does not exist, got nil")
	}
}

func TestCommitRoot_existingRootReturnsError(t *testing.T) {
	db := openTemp(t)

	finalRoot := "/extractions/final"
	db.RecordAll(finalRoot, []filedb.Entry{
		&entry{digest: "sha256:aaa", relPaths: []string{"bin/x"}},
	})
	db.RecordAllStaged("stage1", []filedb.Entry{
		&entry{digest: "sha256:bbb", relPaths: []string{"bin/y"}},
	})

	if err := db.CommitRoot("stage1", finalRoot); err == nil {
		t.Fatal("expected error when committing to an existing root, got nil")
	}
}

func TestRemoveStaging_cleansUpWithoutAffectingChecksums(t *testing.T) {
	db := openTemp(t)

	db.RecordAllStaged("stage1", []filedb.Entry{
		&entry{digest: "sha256:aaa", relPaths: []string{"bin/x"}},
	})
	if err := db.RemoveStaging("stage1"); err != nil {
		t.Fatalf("RemoveStaging: %v", err)
	}
	if db.StagingExists("stage1") {
		t.Error("staging bucket must be removed after RemoveStaging")
	}
	paths, _ := db.PathsForChecksum("sha256:aaa")
	if len(paths) != 0 {
		t.Errorf("checksums must be unaffected by RemoveStaging, got %v", paths)
	}
}

func TestRemoveStaging_nonexistentIsNoop(t *testing.T) {
	db := openTemp(t)
	if err := db.RemoveStaging("does-not-exist"); err != nil {
		t.Fatalf("RemoveStaging on unknown ID: %v", err)
	}
}
