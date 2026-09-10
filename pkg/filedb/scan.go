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
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type scannedEntry struct {
	digest  string
	relPath string
}

func (e *scannedEntry) Digest() string     { return e.digest }
func (e *scannedEntry) RelPaths() []string { return []string{e.relPath} }
func (e *scannedEntry) IsTemporary() bool  { return false }

// ScanRoot walks root and returns one Entry per regular, non-empty file using
// the same digest format as zstd-chunked ("sha256:<hex>" of raw file bytes).
// Zero-length files, symlinks, directories, and other non-regular entries are
// omitted, matching the behaviour of zstd-chunked TOC entries.
func ScanRoot(root string) ([]Entry, error) {
	var entries []Entry
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		digest, err := digestFile(path)
		if err != nil {
			return err
		}
		if digest == "" {
			return nil
		}
		entries = append(entries, &scannedEntry{digest: digest, relPath: relPath})
		return nil
	})
	return entries, err
}

func digestFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	return fmt.Sprintf("sha256:%x", h.Sum(nil)), nil
}
