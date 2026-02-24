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
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/davidcassany/ocistore/pkg/logger"
	"github.com/klauspost/compress/zstd"
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
	resolver := docker.NewResolver(docker.ResolverOptions{})

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
	c.log.Debugf("manifest labels: %v", mfst.Annotations)

	diffIDs, err := images.RootFS(ctx, c.cli.ContentStore(), mfst.Config)
	if err != nil {
		return fmt.Errorf("could not retrieve diffIDs from store: %w", err)
	}
	sn := c.cli.SnapshotService(c.driver)

	err = unpackRemoteLayers(ctx, fetcher, sn, mfst, diffIDs)
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

// fetchMetadata uses the fetcher to grab a blob and returns blob bytes.
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

func unpackRemoteLayers(ctx context.Context, fetcher remotes.Fetcher, sn snapshots.Snapshotter, manifest ocispec.Manifest, diffIDs []digest.Digest) (err error) {
	//var parentChainID digest.Digest
	//var currentChainID digest.Digest

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

		// 6. Fetch the compressed layer blob directly from the network
		rc, err := fetcher.Fetch(ctx, layerDesc)
		if err != nil {
			sn.Remove(ctx, activeKey) // Clean up the active transaction on error
			return fmt.Errorf("failed to fetch layer %s: %w", layerDesc.Digest, err)
		}

		var uncompressedStream io.ReadCloser

		switch layerDesc.MediaType {
		case ocispec.MediaTypeImageLayerZstd:
			dec, err := zstd.NewReader(rc)
			if err != nil {
				rc.Close()
				return err
			}
			uncompressedStream = readCloser{
				Reader: dec,
				close:  dec.Close,
			}
		case ocispec.MediaTypeImageLayerGzip:
			uncompressedStream, err = gzip.NewReader(rc)
			if err != nil {
				rc.Close()
				return err
			}
		default:
			return fmt.Errorf("unexpected layer media type: %s", layerDesc.MediaType)
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
