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
	"io"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/containerd/platforms"
	"github.com/davidcassany/ocistore/pkg/logger"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/semaphore"
)

const maxConcurrentDownloads = 1

type ImageExtractor interface {
	ExtractImage(imageRef, destination, platformRef string, local bool, verify bool) (string, error)
}

type Extractor struct {
	ctx      context.Context
	log      logger.Logger
	platform platforms.MatchComparer
}

func NewExtractor(ctx context.Context, log logger.Logger) Extractor {
	return Extractor{log: log, platform: platforms.DefaultStrict(), ctx: ctx}
}

func (e Extractor) ExtractImage(imageRef, destination, platformRef string, local bool, verify bool) (string, error) {
	resolver := docker.NewResolver(docker.ResolverOptions{})

	name, desc, err := resolver.Resolve(e.ctx, imageRef)
	if err != nil {
		e.log.Errorf("failed resolving image reference into a name and OCI descriptor: %v", err)
		return "", err
	}

	fetcher, err := resolver.Fetcher(e.ctx, name)
	if err != nil {
		e.log.Errorf("failed to set a fetcher for the resolved name '%s': %v", name, err)
		return "", err
	}

	var (
		handler images.Handler
		//		isConvertible         bool
		//		originalSchema1Digest string
		//		converterFunc         func(context.Context, ocispec.Descriptor) (ocispec.Descriptor, error)
		limiter *semaphore.Weighted
	)

	limiter = semaphore.NewWeighted(int64(maxConcurrentDownloads))

	/*
		1. children
		2. filter platform
		3. fetch
	*/

	/*if desc.MediaType == images.MediaTypeDockerSchema1Manifest && rCtx.ConvertSchema1 {

	} else {*/

	handler = images.Handlers(images.FilterPlatforms(fetchHandler(e.log, fetcher, destination), e.platform))
	//}

	if err := images.Dispatch(e.ctx, handler, limiter, desc); err != nil {
		e.log.Errorf("failed on image dispatch: %v", err)
		return "", err
	}

	return string(desc.Digest), nil
}

func fetchHandler(log logger.Logger, fetcher remotes.Fetcher, root string) images.HandlerFunc {
	return func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		log.Infof("digest: %s", desc.Digest.String())
		log.Infof("mediatype: %s", desc.MediaType)
		log.Infof("size: %d", desc.Size)

		if desc.MediaType == images.MediaTypeDockerSchema1Manifest {
			return nil, fmt.Errorf("%v not supported", desc.MediaType)
		}
		return fetch(ctx, fetcher, desc, root)
	}
}

// fetch fetches the given digest into the provided ingester
func fetch(ctx context.Context, fetcher remotes.Fetcher, desc ocispec.Descriptor, root string) ([]ocispec.Descriptor, error) {

	rc, err := fetcher.Fetch(ctx, desc)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	if images.IsLayerType(desc.MediaType) {
		urc, err := compression.DecompressStream(rc)
		if err != nil {
			return nil, err
		}
		_, err = archive.Apply(ctx, root, urc) // which options?
		return nil, err
	} else if images.IsManifestType(desc.MediaType) || images.IsIndexType(desc.MediaType) {
		var data []byte
		if desc.Size == int64(len(desc.Data)) {
			data = desc.Data
		} else {
			data, err = io.ReadAll(rc)
			if err != nil {
				return nil, err
			}
		}
		if images.IsManifestType(desc.MediaType) {
			var manifest ocispec.Manifest
			if err := json.Unmarshal(data, &manifest); err != nil {
				return nil, err
			}
			return append([]ocispec.Descriptor{manifest.Config}, manifest.Layers...), nil
		}
		var index ocispec.Index
		if err := json.Unmarshal(desc.Data, &index); err != nil {
			return nil, err
		}

		return append([]ocispec.Descriptor{}, index.Manifests...), nil
	} else if !images.IsKnownConfig(desc.MediaType) {
		// Log unknown config
	}

	return nil, nil
}
