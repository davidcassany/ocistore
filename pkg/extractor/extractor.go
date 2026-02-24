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
	"context"
	"encoding/json"
	"fmt"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/containerd/platforms"
	"github.com/davidcassany/ocistore/pkg/logger"
	"github.com/davidcassany/ocistore/pkg/ocistore"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type Extractor struct {
	ctx      context.Context
	log      logger.Logger
	platform platforms.MatchComparer
}

func NewExtractor(ctx context.Context, log logger.Logger) Extractor {
	return Extractor{log: log, platform: platforms.DefaultStrict(), ctx: ctx}
}

type metadata struct {
	mfst *ocispec.Manifest
	conf *ocispec.Image
}

func (e Extractor) ExtractImage(imageRef, destination, platformRef string, local bool, verify bool) (string, error) {
	// TODO verify it handles authorization
	resolver := docker.NewResolver(docker.ResolverOptions{})

	name, desc, err := resolver.Resolve(e.ctx, imageRef)
	if err != nil {
		e.log.Errorf("failed resolving image reference into a name and OCI descriptor: %v", err)
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
		fetchManifestAndConfig(e.log, fetcher, &imgMeta),
		e.platform),
	)

	if err := images.Dispatch(e.ctx, handler, nil, desc); err != nil {
		return "", fmt.Errorf("failed on image dispatch: %w", err)
	}

	if imgMeta.mfst == nil || imgMeta.conf == nil {
		return "", fmt.Errorf("failed to find manifest and image config")
	}

	diffIDs := imgMeta.conf.RootFS.DiffIDs
	chainIDs := make([]digest.Digest, len(diffIDs))
	copy(chainIDs, diffIDs)
	chainIDs = identity.ChainIDs(chainIDs)

	for _, layerDesc := range imgMeta.mfst.Layers {
		rc, err := fetcher.Fetch(e.ctx, layerDesc)
		if err != nil {
			return "", fmt.Errorf("failed to fetch layer %s: %w", layerDesc.Digest, err)
		}
		//var uncompressedStream io.ReadCloser
		uncompressedStream, err := compression.DecompressStream(rc)
		if err != nil {
			return "", err
		}

		// TODO handle whiteouts in some special way?
		opts := []archive.ApplyOpt{}
		_, err = archive.Apply(e.ctx, destination, uncompressedStream, opts...)
		uErr := uncompressedStream.Close()
		if err == nil && uErr != nil {
			err = uErr
		}
		cErr := rc.Close()
		if err == nil && cErr != nil {
			err = cErr
		}
		if err != nil {
			return "", fmt.Errorf("failed to apply layer %s: %w", layerDesc.Digest, err)
		}
	}

	return string(desc.Digest), nil
}

func fetchManifestAndConfig(log logger.Logger, fetcher remotes.Fetcher, metadata *metadata) images.HandlerFunc {
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
			log.Debugf("Fetched index manifest with digest: %s", desc.Digest)
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

			log.Debugf("Fetched image manifest with digest: %s", desc.Digest)
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
			log.Debugf("Fetched image config with digest: %s", desc.Digest)
			return nil, nil
		case images.IsLayerType(desc.MediaType):
			log.Debugf("encountered a layer type, not fetching it")
		default:
			log.Debugf("encountered unknown type %v; children may not be fetched", desc.MediaType)
		}
		return nil, nil
	}
}
