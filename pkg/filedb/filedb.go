/*
Copyright © 2026 SUSE LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package filedb

import (
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var (
	bktChecksums = []byte("checksums")
	bktRoots     = []byte("roots")
	bktStaging   = []byte("staging")
)

// DB is the extraction-tracking store.
type DB struct {
	db *bolt.DB
}

// Open opens (or creates) a filedb at path.
//
// Bucket layout
//
//	checksums/          top-level bucket
//	  <digest>/         sub-bucket per content digest
//	    <absPath> → ""  one key per extraction destination
//
//	roots/              top-level bucket
//	  <root>/           sub-bucket per extraction root directory
//	    <relPath> → <digest>  reverse index used by RemoveRoot
func Open(path string) (*DB, error) {
	err := os.MkdirAll(filepath.Dir(path), 0o755)
	if err != nil {
		return nil, fmt.Errorf("creating the path for files database %q: %w", path, err)
	}

	db, err := bolt.Open(path, 0644, nil)
	if err != nil {
		return nil, fmt.Errorf("opening filedb: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bktChecksums, bktRoots, bktStaging} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing filedb: %w", err)
	}
	return &DB{db: db}, nil
}

// Close releases the database file.
func (d *DB) Close() error {
	return d.db.Close()
}

// Entry pairs a content digest with the relative paths of extracted files,
// matching the Digest and Name fields of chunked.FileMetadata.
type Entry interface {
	Digest() string
	RelPaths() []string
	IsTemporary() bool
}

// RecordAll records all entries from a single layer extraction under root in
// one transaction. Entries with an empty Digest are silently skipped
// (directories, symlinks, zero-length files, etc.).
func (d *DB) RecordAll(root string, entries []Entry) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		cb := tx.Bucket(bktChecksums)
		rb := tx.Bucket(bktRoots)

		rsb, err := rb.CreateBucketIfNotExists([]byte(root))
		if err != nil {
			return fmt.Errorf("creating root bucket for %s: %w", root, err)
		}

		for _, e := range entries {
			if e.Digest() == "" || e.IsTemporary() {
				continue
			}

			sb, err := cb.CreateBucketIfNotExists([]byte(e.Digest()))
			if err != nil {
				return fmt.Errorf("creating checksum bucket for %s: %w", e.Digest(), err)
			}
			for _, rp := range e.RelPaths() {
				if rp == "" {
					continue
				}

				absPath := filepath.Join(root, rp)
				if err := sb.Put([]byte(absPath), nil); err != nil {
					return err
				}
				if err := rsb.Put([]byte(rp), []byte(e.Digest())); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// PathsForChecksum returns all absolute paths to which the given digest has
// been extracted. Returns an empty (non-nil) slice when the digest is unknown.
func (d *DB) PathsForChecksum(digest string) ([]string, error) {
	paths := []string{}
	err := d.db.View(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bktChecksums).Bucket([]byte(digest))
		if sb == nil {
			return nil
		}
		return sb.ForEach(func(k, _ []byte) error {
			paths = append(paths, string(k))
			return nil
		})
	})
	return paths, err
}

// StagedPathsForChecksum returns all relative paths to which the given digest has
// been extracted. Returns an empty (non-nil) slice when the digest is unknown.
func (d *DB) StagedPathsForChecksum(stagingID, digest string) ([]string, error) {
	paths := []string{}
	err := d.db.View(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bktStaging).Bucket([]byte(stagingID))
		if sb == nil {
			return nil
		}
		dsb := sb.Bucket([]byte(digest))
		if dsb == nil {
			return nil
		}
		return dsb.ForEach(func(k, _ []byte) error {
			paths = append(paths, string(k))
			return nil
		})
	})
	return paths, err
}

// RootExists returns true if the root bucket for the given root already exists.
// It can be used to verify if the given root is already tracked.
func (d *DB) RootExists(root string) bool {
	var found bool
	_ = d.db.View(func(tx *bolt.Tx) error {
		rsb := tx.Bucket(bktRoots).Bucket([]byte(root))
		if rsb == nil {
			return nil
		}
		found = true
		return nil
	})
	return found
}

// RemoveRoot deletes all records for the given extraction root and
// garbage-collects any digest entries that are no longer referenced by any path.
// It is a no-op when root has never been recorded.
func (d *DB) RemoveRoot(root string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		rb := tx.Bucket(bktRoots)
		rsb := rb.Bucket([]byte(root))
		if rsb == nil {
			return nil
		}

		cb := tx.Bucket(bktChecksums)
		if err := rsb.ForEach(func(relPath, digest []byte) error {
			absPath := filepath.Join(root, string(relPath))
			sb := cb.Bucket(digest)
			if sb == nil {
				return nil
			}
			if err := sb.Delete([]byte(absPath)); err != nil {
				return err
			}
			if k, _ := sb.Cursor().First(); k == nil {
				return cb.DeleteBucket(digest)
			}
			return nil
		}); err != nil {
			return err
		}

		return rb.DeleteBucket([]byte(root))
	})
}

// RecordAllStaged records entries under a staging ID without touching the
// checksums bucket. Call CommitRoot to promote a staged set to its final root,
// writing the checksums entries only at that point.
// Entries with an empty Digest are silently skipped.
func (d *DB) RecordAllStaged(stagingID string, entries []Entry) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		ssb, err := tx.Bucket(bktStaging).CreateBucketIfNotExists([]byte(stagingID))
		if err != nil {
			return fmt.Errorf("creating staging bucket for %s: %w", stagingID, err)
		}
		for _, e := range entries {
			if e.Digest() == "" || e.IsTemporary() {
				continue
			}
			csb, err := ssb.CreateBucketIfNotExists([]byte(e.Digest()))
			if err != nil {
				return fmt.Errorf("creating checksum bucket for %s: %w", e.Digest(), err)
			}
			for _, rp := range e.RelPaths() {
				if rp == "" {
					continue
				}
				if err := csb.Put([]byte(rp), nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// CommitRoot promotes a staging set to its final root in one transaction:
// it writes all staged relPath→digest entries into roots/<finalRoot> and
// their absolute paths into checksums/<digest>/, then drops the staging bucket.
// Returns an error if stagingID does not exist or finalRoot is already tracked.
func (d *DB) CommitRoot(stagingID, finalRoot string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		stb := tx.Bucket(bktStaging)
		ssb := stb.Bucket([]byte(stagingID))
		if ssb == nil {
			return fmt.Errorf("staging %q not found", stagingID)
		}

		rb := tx.Bucket(bktRoots)
		rsb, err := rb.CreateBucket([]byte(finalRoot))
		if err != nil {
			return fmt.Errorf("creating root bucket for %s: %w", finalRoot, err)
		}

		cb := tx.Bucket(bktChecksums)
		if err := ssb.ForEachBucket(func(digest []byte) error {
			dsb := ssb.Bucket(digest)
			if err := dsb.ForEach(func(relPath, _ []byte) error {
				csb, err := cb.CreateBucketIfNotExists(digest)
				if err != nil {
					return fmt.Errorf("creating checksum bucket for %s: %w", string(digest), err)
				}
				absPath := []byte(filepath.Join(finalRoot, string(relPath)))
				if err := csb.Put(absPath, nil); err != nil {
					return err
				}
				return rsb.Put(relPath, digest)
			}); err != nil {
				return fmt.Errorf("iterating staged paths: %w", err)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("iterating staged sub-buckets: %w", err)
		}
		return stb.DeleteBucket([]byte(stagingID))
	})
}

// StagingExists returns true if a staging bucket for the given ID exists.
func (d *DB) StagingExists(stagingID string) bool {
	var found bool
	_ = d.db.View(func(tx *bolt.Tx) error {
		found = tx.Bucket(bktStaging).Bucket([]byte(stagingID)) != nil
		return nil
	})
	return found
}

// RemoveStaging drops a staging bucket without writing anything to the checksums
// or roots indexes. It is a no-op when stagingID has never been recorded.
func (d *DB) RemoveStaging(stagingID string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket(bktStaging)
		if sb.Bucket([]byte(stagingID)) == nil {
			return nil
		}
		return sb.DeleteBucket([]byte(stagingID))
	})
}
