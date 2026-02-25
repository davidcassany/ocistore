/*
Copyright © 2024-2026 SUSE LLC

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

package ocistore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/containerd/stargz-snapshotter/estargz"
	"github.com/containerd/stargz-snapshotter/estargz/zstdchunked"
	"github.com/davidcassany/ocistore/pkg/logger"
	"github.com/moby/sys/userns"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

func (c *OCIStore) RemoteUnpack(ref string, opts ...ApplyCommitOpt) (err error) {
	if !c.IsInitiated() {
		return errors.New(missInitErrMsg)
	}

	ctx, done, err := c.cli.WithLease(c.ctx, leases.WithRandomID(), leases.WithExpiration(1*time.Hour))
	if err != nil {
		c.log.Errorf("failed to create lease for unpacking '%s': %v", ref, err)
		return err
	}
	defer func() {
		dErr := done(ctx)
		if dErr != nil && err == nil {
			c.log.Warnf("could not remove lease for unpack operation")
		}
	}()

	// TODO verify it handles authorization
	resolver := setupResolver()

	name, desc, err := resolver.Resolve(c.ctx, ref)
	if err != nil {
		c.log.Errorf("failed resolving image reference into a name and OCI descriptor: %v", err)
		return err
	}

	fetcher, err := resolver.Fetcher(ctx, name)
	if err != nil {
		return fmt.Errorf("initiating fetcher for image %s: %w", name, err)
	}
	platformMatcher := platforms.DefaultStrict()

	// 2. Create a handler that ONLY fetches metadata (Manifests and Configs)
	// and entirely ignores layer blobs.
	handler := images.Handlers(
		images.FilterPlatforms(
			images.SetChildrenLabels(
				c.cli.ContentStore(), remoteChildren(c.log, c.cli.ContentStore(), fetcher),
			), platformMatcher,
		),
	)

	// 3. Dispatch the handler starting from the root descriptor (the Index)
	err = images.Dispatch(ctx, handler, nil, desc)

	img := images.Image{
		Name:   name,
		Target: desc,
		// TODO figure out if we need some additional labels
	}

	is := c.cli.ImageService()
	for {
		if created, err := is.Create(ctx, img); err != nil {
			if !errdefs.IsAlreadyExists(err) {
				return err
			}

			updated, err := is.Update(ctx, img)
			if err != nil {
				// if image was removed, try create again
				if errdefs.IsNotFound(err) {
					continue
				}
				return err
			}
			img = updated
		} else {
			img = created
		}
		break
	}

	mfst, err := images.Manifest(ctx, c.cli.ContentStore(), desc, platformMatcher)
	if err != nil {
		return fmt.Errorf("could not retrieve manifest from store: %w", err)
	}

	diffIDs, err := images.RootFS(ctx, c.cli.ContentStore(), mfst.Config)
	if err != nil {
		return fmt.Errorf("could not retrieve diffIDs from store: %w", err)
	}
	sn := c.cli.SnapshotService(c.driver)

	err = unpackRemoteLayers(ctx, c.log, fetcher, sn, mfst, diffIDs)
	if err != nil {
		return fmt.Errorf("could not unpack layers: %w", err)
	}

	return err
}

func remoteChildren(log logger.Logger, store content.Store, fetcher remotes.Fetcher) images.HandlerFunc {
	return func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		switch {
		case images.IsDockerType(desc.MediaType):
			return nil, fmt.Errorf("%v not supported", desc.MediaType)
		case images.IsIndexType(desc.MediaType):
			var index ocispec.Index

			err := fetchAndStoreMetadata(ctx, fetcher, store, desc, &index)
			if err != nil {
				return nil, fmt.Errorf("failed fetching or storing index: %w", err)
			}

			log.Debugf("Stored index manifest with digest: %s", desc.Digest)
			return append([]ocispec.Descriptor{}, index.Manifests...), nil
		case images.IsManifestType(desc.MediaType):
			var manifest ocispec.Manifest

			err := fetchAndStoreMetadata(ctx, fetcher, store, desc, &manifest)
			if err != nil {
				return nil, fmt.Errorf("failed fetching or storing manifest: %w", err)
			}

			log.Debugf("Stored image manifest with digest: %s", desc.Digest)
			return append([]ocispec.Descriptor{manifest.Config}, manifest.Layers...), nil
		case images.IsConfigType(desc.MediaType):
			var config ocispec.Image

			err := fetchAndStoreMetadata(ctx, fetcher, store, desc, &config)
			if err != nil {
				return nil, fmt.Errorf("failed fetching or storing manifest: %w", err)
			}

			log.Debugf("Stored image config with digest: %s", desc.Digest)
			return nil, nil
		case images.IsLayerType(desc.MediaType):
			log.Debugf("encountered a layer type, not fetching it")
		default:
			log.Debugf("encountered unknown type %v; children may not be fetched", desc.MediaType)
		}
		return nil, nil
	}
}

// fetchAndStoreMetadata uses the fetcher to grab a blob and writes it to the content store.
// Metadata contents are deserialized to the given metadata pointer (Manifest or Config).
// This is strictly used for the small non-layer blobs (Manifest and Config).
func fetchAndStoreMetadata(ctx context.Context, fetcher remotes.Fetcher, store content.Store, desc ocispec.Descriptor, metadata any) error {
	// Fetch from the registry
	metadataBytes, err := FetchMetadata(ctx, fetcher, desc)
	if err != nil {
		return nil
	}

	if metadata != nil {
		// Unmarshal config or manifest
		if err := json.Unmarshal(metadataBytes, metadata); err != nil {
			return fmt.Errorf("unmarshalling metadata error: %w", err)
		}
	}

	// Commit it to containerd's content store
	err = content.WriteBlob(ctx, store, desc.Digest.String(), bytes.NewReader(metadataBytes), desc)
	if err != nil {
		return fmt.Errorf("failed to write blob to content store: %w", err)
	}

	return nil
}

// FetchMetadata uses the fetcher to grab a blob and returns blob bytes.
// This is strictly used for the small non-layer blobs (Manifest and Config).
func FetchMetadata(ctx context.Context, fetcher remotes.Fetcher, desc ocispec.Descriptor) (metadata []byte, err error) {
	// Fetch from the registry
	rc, err := fetcher.Fetch(ctx, desc)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata blob %s: %w", desc.Digest, err)
	}
	defer func() {
		cerr := rc.Close()
		if err == nil && cerr != nil {
			err = cerr
		}
	}()

	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading remote io Reader: %w", err)
	}
	return b, nil
}

func unpackRemoteLayers(ctx context.Context, log logger.Logger, fetcher remotes.Fetcher, sn snapshots.Snapshotter, manifest ocispec.Manifest, diffIDs []digest.Digest) (err error) {
	chainIDs := make([]digest.Digest, len(diffIDs))
	copy(chainIDs, diffIDs)
	chainIDs = identity.ChainIDs(chainIDs)

	var parentChainID, currentChainID string

	for i, layerDesc := range manifest.Layers {
		if i > 0 {
			parentChainID = chainIDs[i-1].String()
		}
		currentChainID = chainIDs[i].String()

		// Check if this layer has already been unpacked in the past
		if _, err := sn.Stat(ctx, currentChainID); err == nil {
			parentChainID = currentChainID
			continue // Skip download, snapshot already exists
		}

		// 5. Prepare a new snapshot directory
		activeKey := fmt.Sprintf("extract-%s", currentChainID)
		mounts, err := sn.Prepare(ctx, activeKey, parentChainID)
		if err != nil {
			return fmt.Errorf("failed to prepare snapshot: %w", err)
		}

		// fetch TOC for zstd:chunked formatted layers
		_, _, err = fetchZstdTOC(ctx, log, layerDesc, fetcher)
		if err != nil {
			log.Warnf("could not fetch zstd:chunked's TOC, skip TOC fetch: %v", err)
		}

		// 6. Fetch the compressed layer blob directly from the network
		rc, err := fetcher.Fetch(ctx, layerDesc)
		if err != nil {
			sn.Remove(ctx, activeKey) // Clean up the active transaction on error
			return fmt.Errorf("failed to fetch layer %s: %w", layerDesc.Digest, err)
		}

		uncompressedStream, err := compression.DecompressStream(rc)
		if err != nil {
			return fmt.Errorf("setting the uncompressed stream reader: %w", err)
		}

		// 7. Apply the tar stream directly to the snapshotter's mounts
		err = applyUnix(ctx, mounts, uncompressedStream, true)
		uErr := uncompressedStream.Close()
		if err == nil && uErr != nil {
			err = uErr
		}
		cErr := rc.Close()
		if err == nil && cErr != nil {
			err = cErr
		}
		if err != nil {
			sn.Remove(ctx, activeKey)
			return fmt.Errorf("failed to apply layer %s: %w", layerDesc.Digest, err)
		}

		// 8. Commit the snapshot to lock it into the immutable state
		// Label added here is just to be consistent with unpacked images, I don't know the motivation of this label
		if err = sn.Commit(ctx, currentChainID, activeKey, snapshots.WithLabels(map[string]string{
			"containerd.io/snapshot.ref": currentChainID,
		})); err != nil {
			if errdefs.IsAlreadyExists(err) {
				return nil
			}
			return err
		}

		// Step forward
		parentChainID = currentChainID
		fmt.Printf("Successfully unpacked layer: %s\n", currentChainID)
	}
	return nil
}

type readCloser struct {
	io.Reader
	close func()
}

func (r readCloser) Close() error {
	r.close()
	return nil
}

// Define a custom type for our context key to avoid collisions
type rangeContextKey struct{}

// rangeTarget holds the instructions for our RoundTripper
type rangeTarget struct {
	Digest string
	Offset int64
	Length int64
}

// rangeRoundTripper intercepts requests and injects the Range header
type rangeRoundTripper struct {
	Base http.RoundTripper
}

func (rt *rangeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Check if the range target instructions are in the context
	target, ok := req.Context().Value(rangeContextKey{}).(rangeTarget)
	if !ok {
		// Not our target, proceed normally
		return rt.Base.RoundTrip(req)
	}

	// Check the request is a GET method including the Digest we want to fetch, then
	// assume this the request we want to intercept and recreate
	if strings.Contains(req.URL.Path, target.Digest) && req.Method == http.MethodGet {
		clonedReq := req.Clone(req.Context())
		bRange := fmt.Sprintf("bytes=%d-%d", target.Offset, target.Offset+target.Length-1)
		clonedReq.Header.Set("Range", bRange)
		return rt.Base.RoundTrip(clonedReq)
	}
	return rt.Base.RoundTrip(req)
}

// setupResolver creates a new resolver with a custom http client with the http.RoundTripper
// to support ranged http requests.
// TODO: expose resolver configuration options as optional parametres
func setupResolver() remotes.Resolver {
	customClient := &http.Client{
		Transport: &rangeRoundTripper{
			Base: http.DefaultTransport,
		},
	}

	// Initialize the resolver with our customized client
	opts := docker.ResolverOptions{
		Client: customClient,
		// You can also add your registry hosts / auth configurations here
		// TODO: I don't know why adding this causes to ignore the custom client
		//Hosts: docker.ConfigureDefaultRegistries(),
	}

	return docker.NewResolver(opts)
}

func fetchRange(ctx context.Context, fetcher remotes.Fetcher, desc ocispec.Descriptor, offset int64, length int64) (io.ReadCloser, error) {
	/*fetcher, err := resolver.Fetcher(ctx, ref)
	if err != nil {
		return nil, err
	}*/

	target := rangeTarget{
		Digest: desc.Digest.Encoded(),
		Offset: offset,
		Length: length,
	}
	ctxWithRange := context.WithValue(ctx, rangeContextKey{}, target)

	rc, err := fetcher.Fetch(ctxWithRange, desc)
	if err != nil {
		return nil, err
	}

	return rc, nil
}

func uncompressBytes(tocBytes []byte) ([]byte, error) {
	decoder, err := compression.DecompressStream(bytes.NewReader(tocBytes))
	if err != nil {
		return nil, fmt.Errorf("decoding TOC bytes: %w", err)
	}
	defer decoder.Close()

	uncompressedTOC, err := io.ReadAll(decoder)
	if err != nil {
		return nil, fmt.Errorf("reading decompressed TOC: %w", err)
	}

	return uncompressedTOC, nil
}

// TODO ensure the fetcher includes our custom RoundTripper, probably define our own type alias for the fetcher
func fetchZstdTOC(ctx context.Context, log logger.Logger, layerDesc ocispec.Descriptor, fetcher remotes.Fetcher) (*estargz.JTOC, digest.Digest, error) {
	var tocDgst digest.Digest

	if layerDesc.MediaType != ocispec.MediaTypeImageLayerZstd {
		// We do nothing if the layer is not of zstd type
		log.Debugf("%s is not a zstd layer, no TOC to fecth", layerDesc.Digest.String())
		return nil, tocDgst, nil
	}

	if val, ok := layerDesc.Annotations[zstdchunked.ManifestChecksumAnnotation]; ok {
		log.Debugf("annotation points to zstd:chunked format. Checksum: %s", val)
	}

	decompressor := new(zstdchunked.Decompressor)
	footerSize := decompressor.FooterSize()

	rc, err := fetchRange(ctx, fetcher, layerDesc, layerDesc.Size-footerSize, footerSize)
	if err != nil {
		return nil, tocDgst, fmt.Errorf("fetching blob footer: %w", err)
	}
	footerBytes, err := io.ReadAll(rc)
	cErr := rc.Close()
	if err != nil {
		return nil, tocDgst, fmt.Errorf("reading zstd footer bytes: %w", err)
	}
	if cErr != nil {
		return nil, tocDgst, fmt.Errorf("closing footer reader: %w", err)
	}

	_, tocOff, tocSize, err := decompressor.ParseFooter(footerBytes)
	if err != nil {
		// We assume this is not of zstd:chunked type and process normally
		log.Debugf("could not parse zstd TOC from footer: %w", err)
		return nil, tocDgst, nil
	}

	if tocSize <= 0 {
		log.Warnf("inconsistent TOC size (%d) detected, ignoring TOC", tocSize)
		return nil, tocDgst, nil
	}

	rc, err = fetchRange(ctx, fetcher, layerDesc, tocOff, tocSize)
	if err != nil {
		return nil, tocDgst, fmt.Errorf("fetching TOC: %w", err)
	}

	toc, tocDgst, err := decompressor.ParseTOC(rc)
	cErr = rc.Close()
	if err != nil {
		return nil, tocDgst, fmt.Errorf("decompressing TOC: %w", err)
	}
	if cErr != nil {
		return nil, tocDgst, fmt.Errorf("closing TOC reader: %w", err)
	}
	log.Debugf("uncompressed TOC with %d entries and digest %s", len(toc.Entries), tocDgst.String())

	return toc, tocDgst, nil
}

// From here up to the and of the file the code is copied form containerd
// https://github.com/containerd/containerd/blob/main/core/diff/apply/apply_linux.go
// There is no public differ API to apply changes from a Reader. In fact this is just
// a small wrapper around archive.Apply which is public, this handles some specific corner
// cases which are related to the underlaying differ and snapshotter.

func applyUnix(ctx context.Context, mounts []mount.Mount, r io.Reader, sync bool) (retErr error) {
	switch {
	case len(mounts) == 1 && mounts[0].Type == "overlay":
		// OverlayConvertWhiteout (mknod c 0 0) doesn't work in userns.
		// https://github.com/containerd/containerd/issues/3762
		if userns.RunningInUserNS() {
			break
		}
		path, parents, err := getOverlayPath(mounts[0].Options)
		if err != nil {
			if errdefs.IsInvalidArgument(err) {
				break
			}
			return err
		}
		opts := []archive.ApplyOpt{
			archive.WithConvertWhiteout(archive.OverlayConvertWhiteout),
		}
		if len(parents) > 0 {
			opts = append(opts, archive.WithParents(parents))
		}
		_, err = archive.Apply(ctx, path, r, opts...)
		if err == nil && sync {
			err = doSyncFs(path)
		}
		return err
	case sync && len(mounts) == 1 && mounts[0].Type == "bind":
		defer func() {
			if retErr != nil {
				return
			}

			retErr = doSyncFs(mounts[0].Source)
		}()
	}
	return mount.WithTempMount(ctx, mounts, func(root string) error {
		_, err := archive.Apply(ctx, root, r)
		return err
	})
}

func getOverlayPath(options []string) (upper string, lower []string, err error) {
	const upperdirPrefix = "upperdir="
	const lowerdirPrefix = "lowerdir="

	for _, o := range options {
		if strings.HasPrefix(o, upperdirPrefix) {
			upper = strings.TrimPrefix(o, upperdirPrefix)
		} else if strings.HasPrefix(o, lowerdirPrefix) {
			lower = strings.Split(strings.TrimPrefix(o, lowerdirPrefix), ":")
		}
	}
	if upper == "" {
		return "", nil, fmt.Errorf("upperdir not found: %w", errdefs.ErrInvalidArgument)
	}

	return
}

func doSyncFs(file string) error {
	fd, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", file, err)
	}
	defer fd.Close()

	err = unix.Syncfs(int(fd.Fd()))
	if err != nil {
		return fmt.Errorf("failed to syncfs for %s: %w", file, err)
	}
	return nil
}
