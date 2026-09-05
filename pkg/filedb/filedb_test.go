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
	digest   string
	relPaths []string
}

func (e entry) Digest() string {
	return e.digest
}

func (e entry) RelPaths() []string {
	return e.relPaths
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
