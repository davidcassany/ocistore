/*
Copyright © 2024 SUSE LLC

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

package extractor

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"uuid"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/containerd/platforms"
	"github.com/davidcassany/ocistore/pkg/chunked"
	"github.com/davidcassany/ocistore/pkg/filedb"
	"github.com/davidcassany/ocistore/pkg/logger"
	"github.com/davidcassany/ocistore/pkg/ocistore"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	DefaultDBPath = ocistore.DefaultRoot + "/files.db"

	whiteout    = ".wh."
	opqWhiteout = ".wh..wh..opq"
	tmpDir      = ".toRemove"
)

type Extractor struct {
	ctx        context.Context
	platform   platforms.MatchComparer
	fileDbPath string
	delta      bool
}

type ExtractorOpt func(e *Extractor)

func WithDBPath(path string) ExtractorOpt {
	return func(e *Extractor) {
		e.fileDbPath = path
	}
}

func WithDelta(delta bool) ExtractorOpt {
	return func(e *Extractor) {
		e.delta = delta
	}
}

func NewExtractor(ctx context.Context, opts ...ExtractorOpt) *Extractor {
	e := &Extractor{
		platform:   platforms.DefaultStrict(),
		ctx:        ctx,
		fileDbPath: DefaultDBPath,
	}

	for _, o := range opts {
		o(e)
	}

	return e
}

type metadata struct {
	mfst *ocispec.Manifest
	conf *ocispec.Image
}

type layerCtx struct {
	// seenPaths collects already applied paths, set to true for no directory paths
	// and set to false for directory paths. Note that a key with a value set to false
	// hase a different meaning than a missing key.
	seenPaths map[string]bool

	// whiteouts collects all whiteouts, opaque or not, if for a given key path
	// it is set to false it means this is not a whiteout
	whiteouts map[string]bool

	// pendingOpqs collects all the opaque whiteouts pending to active (e.g.
	// seen in current layer). Use applyOpaques() method to apply them, this
	// expected to happen between layers.
	pendingOpqs []string

	// hardlinks are all links found in layers so they can be created
	// after extracting them all
	links hardlinks

	// keptWh are files that are supressed form the final extraction but temporarly kept in
	// case they are the source of a hardlink in the same or higher layer. The key is the orginal
	// path and the value the temporary file path used for the extraction.
	keptWh map[string]string
}

func newLayerCtx() *layerCtx {
	return &layerCtx{
		seenPaths:   map[string]bool{},
		whiteouts:   map[string]bool{},
		pendingOpqs: []string{},
		links:       hardlinks{},
		keptWh:      map[string]string{},
	}
}

func (lc *layerCtx) applyOpaques() {
	for _, opq := range lc.pendingOpqs {
		lc.whiteouts[opq] = true
	}
	lc.pendingOpqs = []string{}
}

// The key is the original filepath and the values string slice represents alls the links created from the original file.
type hardlinks map[string][]string

func (e Extractor) ExtractImage(imageRef, destination, platformRef string, local bool, verify bool) (_ string, err error) {
	start := time.Now()

	destination, err = filepath.Abs(destination)
	if err != nil {
		return "", fmt.Errorf("cannot set destination %q as an absolute path: %w", destination, err)
	}

	err = os.MkdirAll(destination, 0755)
	if err != nil {
		return "", fmt.Errorf("creating destination directory: %w", err)
	}

	logger.Debug("Extracting image to %s", destination)

	// TODO check if it handles authorization
	resolver := ocistore.SetupOCIRegistryResolver(verify, nil)

	name, desc, err := resolver.Resolve(e.ctx, imageRef)
	if err != nil {
		logger.Error("failed resolving image reference into a name and OCI descriptor: %v", err)
		return "", err
	}

	fetcher, err := resolver.Fetcher(e.ctx, name)
	if err != nil {
		return "", fmt.Errorf("initiating fetcher for image %s: %w", name, err)
	}

	var (
		handler images.Handler
		imgMeta metadata
	)

	handler = images.Handlers(images.FilterPlatforms(
		fetchManifestAndConfig(fetcher, &imgMeta),
		e.platform),
	)

	if err := images.Dispatch(e.ctx, handler, nil, desc); err != nil {
		return "", fmt.Errorf("failed on image dispatch: %w", err)
	}

	if imgMeta.mfst == nil || imgMeta.conf == nil {
		return "", fmt.Errorf("failed to find manifest and image config")
	}

	digest := imgMeta.mfst.Config.Digest.String()

	lCtx := newLayerCtx()

	var db *filedb.DB
	var toc *chunked.TOC

	if e.delta {
		db, err = filedb.Open(e.fileDbPath)
		if err != nil {
			return "", fmt.Errorf("opening file database: %w", err)
		}
		if db.RootExists(destination) {
			return "", fmt.Errorf("cannot extract data to an already cached root: %q", destination)
		}
		defer func() {
			if err != nil {
				err = errors.Join(err, db.RemoveStaging(destination))
				err = errors.Join(err, db.RemoveRoot(destination))
			}
		}()
	}

	for _, layerDesc := range slices.Backward(imgMeta.mfst.Layers) {
		if e.delta {
			toc, err = ocistore.FetchToC(e.ctx, fetcher, layerDesc)
			if err != nil {
				logger.Warning("could not extract ToC: %s. Fallback to regular extraction", err.Error())
			}
		}

		if toc == nil {
			err = fetchAndApplyLayer(e.ctx, fetcher, layerDesc, destination, lCtx)
			if err != nil {
				return "", err
			}
		} else {
			err = fetchAndApplyDeltaLayer(e.ctx, fetcher, db, toc, layerDesc, destination, lCtx)
			if err != nil {
				return "", err
			}
		}
		err = createHardLinks(lCtx)
		if err != nil {
			return "", fmt.Errorf("creating deferred hardlinks: %w", err)
		}
	}

	if len(lCtx.links) > 0 {
		return "", fmt.Errorf("there are pending links to be created: %v", lCtx.links)
	}

	err = os.RemoveAll(filepath.Join(destination, tmpDir))
	if err != nil {
		return "", fmt.Errorf("removing kept whiteouts: %w", err)
	}

	if e.delta {
		if !db.StagingExists(destination) {
			entries, err := filedb.ScanRoot(destination)
			if err != nil {
				return "", fmt.Errorf("scanning root %s: %w", destination, err)
			}
			err = db.RecordAll(destination, entries)
			if err != nil {
				return "", fmt.Errorf("recording root in to staging db: %w", err)
			}
		} else {
			err = db.CommitRoot(destination, destination)
			if err != nil {
				return "", fmt.Errorf("updating files database for root %s: %w", destination, err)
			}
		}
	}

	logger.Info("Extraction finished in %v", time.Since(start))

	return digest, nil
}

func fetchAndApplyLayer(ctx context.Context, fetcher remotes.Fetcher, layer ocispec.Descriptor, destination string, lCtx *layerCtx) error {
	logger.Debug("starting to fetch layer stream")

	rc, err := fetcher.Fetch(ctx, layer)
	if err != nil {
		return fmt.Errorf("failed to fetch layer %s: %w", layer.Digest, err)
	}

	logger.Debug("decompressing layer stream")

	uncompressedStream, err := compression.DecompressStream(rc)
	if err != nil {
		_ = rc.Close()
		return err
	}

	opts := []archive.ApplyOpt{
		archive.WithFilter(filterFunc(destination, lCtx)),
	}

	logger.Debug("applying uncompressed stream")

	_, err = archive.Apply(ctx, destination, uncompressedStream, opts...)
	err = errors.Join(err, uncompressedStream.Close(), rc.Close())
	if err != nil {
		return fmt.Errorf("failed to apply layer %s: %w", layer.Digest, err)
	}

	// Ensure opaques are honored in any follow up layer
	lCtx.applyOpaques()

	return nil
}

func filterWhiteout(lCtx *layerCtx, relPath string) bool {
	baseName := filepath.Base(relPath)
	dirName := filepath.Dir(relPath)

	if baseName == opqWhiteout {
		lCtx.pendingOpqs = append(lCtx.pendingOpqs, dirName)
		return true
	} else if after, ok := strings.CutPrefix(baseName, whiteout); ok {
		relPath = filepath.Join(dirName, after)
		lCtx.whiteouts[relPath] = true
		return true
	}

	return false
}

func isWhiteout(lCtx *layerCtx, path string) bool {
	if lCtx.whiteouts[path] {
		return true
	}
	dir := filepath.Dir(path)
	for dir != "." && dir != "/" && dir != "" {
		if lCtx.whiteouts[dir] {
			return true
		}
		dir = filepath.Dir(dir)
	}
	return false
}

func updateTarPath(hdr *tar.Header, path string) {
	hdr.Name = path
	if hdr.PAXRecords != nil {
		if _, ok := hdr.PAXRecords["path"]; ok {
			hdr.PAXRecords["path"] = hdr.Name
		}
	}
}

// filterFunc prevents to extract files that are included in the extracted cache and feeds the extracted cache with files being extracted.
// It also intercepts all hardlinks for later processing
func filterFunc(destination string, lCtx *layerCtx) func(hdr *tar.Header) (bool, error) {
	return func(hdr *tar.Header) (bool, error) {
		relPath := filepath.Clean(hdr.Name)
		if relPath == "." || relPath == "/" {
			return true, nil
		}
		absPath := filepath.Join(destination, relPath)

		// filter whiteouts
		if filterWhiteout(lCtx, relPath) {
			return false, nil
		}

		// check if the entry is meant to be ignored
		if isWhiteout(lCtx, relPath) {
			if hdr.Typeflag != tar.TypeReg {
				return false, nil
			}
			updateTarPath(hdr, filepath.Join(tmpDir, uuid.New().String()))
			lCtx.keptWh[absPath] = filepath.Join(destination, hdr.Name)
		}

		// omit if previously seen from an upper layer
		if seen, ok := lCtx.seenPaths[relPath]; ok {
			if seen && hdr.Typeflag == tar.TypeDir {
				lCtx.whiteouts[relPath] = true
			}
			return false, nil
		}

		// mark as seen for subsequent lower layers
		lCtx.seenPaths[relPath] = hdr.Typeflag != tar.TypeDir

		// intercept hardlinks for deferred processing
		if hdr.Typeflag == tar.TypeLink {
			oldpath := filepath.Join(destination, hdr.Linkname)
			if err := ensureSafePath(destination, oldpath); err != nil {
				return false, fmt.Errorf("illegal hardlink target: %w", err)
			}
			lCtx.links[oldpath] = append(lCtx.links[oldpath], absPath)
			return false, nil // Skip native extraction
		}

		// Extract all other unseen files normally
		return true, nil
	}
}

// ensureSafePath checks if the target path is lexically inside the base directory.
func ensureSafePath(baseDir, targetPath string) error {
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return fmt.Errorf("failed to get absolute path of base dir: %v", err)
	}

	absTarget, err := filepath.Abs(targetPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path of target: %v", err)
	}

	// Calculate the relative path from base to target
	rel, err := filepath.Rel(absBase, absTarget)
	if err != nil {
		return fmt.Errorf("failed to calculate relative path: %v", err)
	}

	// If the relative path starts with ".." or is exactly "..", it escapes the base dir
	if strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return fmt.Errorf("path traversal detected: %s escapes %s", targetPath, baseDir)
	}

	return nil
}

func fetchManifestAndConfig(fetcher remotes.Fetcher, metadata *metadata) images.HandlerFunc {
	return func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		switch {
		case images.IsDockerType(desc.MediaType):
			return nil, fmt.Errorf("%s media type not supported", desc.MediaType)
		case images.IsIndexType(desc.MediaType):
			var index ocispec.Index
			metadataBytes, err := ocistore.FetchMetadata(ctx, fetcher, desc)
			if err != nil {
				return nil, fmt.Errorf("failed fetching index: %w", err)
			}
			if err := json.Unmarshal(metadataBytes, &index); err != nil {
				return nil, fmt.Errorf("unmarshalling index error: %w", err)
			}
			logger.Debug("Fetched index manifest with digest: %s", desc.Digest)
			return append([]ocispec.Descriptor{}, index.Manifests...), nil
		case images.IsManifestType(desc.MediaType):
			if metadata.mfst != nil {
				return nil, fmt.Errorf("manifest already defined, there can only be one")
			}
			metadataBytes, err := ocistore.FetchMetadata(ctx, fetcher, desc)
			if err != nil {
				return nil, fmt.Errorf("failed fetching manifest: %w", err)
			}

			var manifest ocispec.Manifest
			if err := json.Unmarshal(metadataBytes, &manifest); err != nil {
				return nil, fmt.Errorf("unmarshalling manifest error: %w", err)
			}
			metadata.mfst = &manifest

			logger.Debug("Fetched image manifest with digest: %s", desc.Digest)
			return append([]ocispec.Descriptor{manifest.Config}, manifest.Layers...), nil
		case images.IsConfigType(desc.MediaType):
			if metadata.conf != nil {
				return nil, fmt.Errorf("config is not zero, there can only be one")
			}

			metadataBytes, err := ocistore.FetchMetadata(ctx, fetcher, desc)
			if err != nil {
				return nil, fmt.Errorf("failed fetching manifest: %w", err)
			}

			var config ocispec.Image
			if err := json.Unmarshal(metadataBytes, &config); err != nil {
				return nil, fmt.Errorf("unmarshalling config error: %w", err)
			}

			metadata.conf = &config
			logger.Debug("Fetched image config with digest: %s", desc.Digest)
			return nil, nil
		case images.IsLayerType(desc.MediaType):
			logger.Debug("encountered a layer type, not fetching it")
		default:
			logger.Debug("encountered unknown type %v; children may not be fetched", desc.MediaType)
		}
		return nil, nil
	}
}

func createHardLinks(lCtx *layerCtx) error {
	pendingLinks := hardlinks{}

	for oldP, newPs := range lCtx.links {
		if lCtx.keptWh[oldP] != "" {
			oldP = lCtx.keptWh[oldP]
		} else if _, err := os.Stat(oldP); err != nil && errors.Is(err, fs.ErrNotExist) {
			pendingLinks[oldP] = newPs
			continue
		}
		for _, newP := range newPs {
			err := os.Link(oldP, newP)
			if err != nil {
				if os.IsExist(err) {
					_ = os.Remove(newP)
					err = os.Link(oldP, newP)
				}
				if err != nil {
					return fmt.Errorf("creating hardlink %s -> %s: %w", newP, oldP, err)
				}
			}
		}
	}
	lCtx.links = pendingLinks
	return nil
}
