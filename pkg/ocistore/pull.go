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

package ocistore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/stargz-snapshotter/estargz/zstdchunked"
	"github.com/davidcassany/ocistore/pkg/logger"
	"github.com/klauspost/compress/zstd"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type PullOpts struct {
	aOpts  []ApplyCommitOpt
	rOpts  []client.RemoteOpt
	unpack bool
}

type PullOpt func(*PullOpts) error

func WithPullClientOpts(opts ...client.RemoteOpt) PullOpt {
	return func(pOpts *PullOpts) error {
		pOpts.rOpts = append(pOpts.rOpts, opts...)
		return nil
	}
}

func WithPullUnpack() PullOpt {
	return func(pOpts *PullOpts) error {
		pOpts.unpack = true
		return nil
	}
}

func WithPullApplyCommitOpts(opts ...ApplyCommitOpt) PullOpt {
	return func(pOpts *PullOpts) error {
		pOpts.aOpts = append(pOpts.aOpts, opts...)
		return nil
	}
}

func (c *OCIStore) Pull(ref string, opts ...PullOpt) (_ client.Image, retErr error) {
	if !c.IsInitiated() {
		return nil, errors.New(missInitErrMsg)
	}

	handler, err := zstdTOCHandler(c.log, ref)
	if err != nil {
		return nil, err
	}

	pOpt := &PullOpts{
		aOpts: []ApplyCommitOpt{},
		rOpts: []client.RemoteOpt{client.WithImageHandler(handler)},
	}
	for _, o := range opts {
		err := o(pOpt)
		if err != nil {
			return nil, err
		}
	}

	ctx, done, err := c.cli.WithLease(c.ctx, leases.WithRandomID(), leases.WithExpiration(1*time.Hour))
	if err != nil {
		c.log.Errorf("failed to create lease to pull image: %v", err)
		return nil, err
	}
	defer func() {
		err = done(ctx)
		if err != nil && retErr == nil {
			c.log.Warnf("could not remove lease on pull operation")
		}
	}()

	imgSt, err := c.cli.Fetch(ctx, ref, pOpt.rOpts...)
	if err != nil {
		c.log.Errorf("failed to pull image '%s': %v", ref, err)
		return nil, err
	}
	img := client.NewImage(c.cli, imgSt)
	c.log.Infof("Successfully pulled image '%s'", img.Name())

	if pOpt.unpack {
		err = c.unpack(ctx, img, pOpt.aOpts...)
		if err != nil {
			c.log.Errorf("failed to unpack image '%s': %v", img.Name(), err)
		} else {
			c.log.Infof("Successfully unpacked image '%s'", img.Name())
		}
	}
	return img, err
}

// zstdTOCHandler returns an images.HandlerFunc that parses the zstd:chunked TOC if any
func zstdTOCHandler(log logger.Logger, ref string) (images.HandlerFunc, error) {
	return func(ctx context.Context, desc ocispec.Descriptor) (subdescs []ocispec.Descriptor, err error) {
		if desc.MediaType == ocispec.MediaTypeImageLayerZstd {
			log.Debugf("--> Discovered media type: %s", desc.MediaType)
			log.Debugf("    Digest: %s", desc.Digest.String())
			log.Debugf("    Size: %d bytes", desc.Size)

			if val, ok := desc.Annotations[zstdchunked.ManifestChecksumAnnotation]; ok {
				log.Info("    Annotation points to zstd:chunked format. Checksum: ", val)
			}
			resolver := setupResolver()
			decompressor := new(zstdchunked.Decompressor)
			footerSize := decompressor.FooterSize()

			footerBytes, err := fetchRange(ctx, resolver, ref, desc, desc.Size-footerSize, footerSize)
			if err != nil {
				log.Warnf("error found when reading range: %s", err.Error())
				return nil, fmt.Errorf("failed to fetch blob footer: %w", err)
			}

			_, tocOff, tocSize, err := decompressor.ParseFooter(footerBytes)
			if err != nil {
				log.Warnf("error found when parsing TOC: %s", err.Error())
				return nil, fmt.Errorf("failed to parse zstdchunked footer: %w", err)
			}

			if tocSize <= 0 {
				return nil, fmt.Errorf("invalid TOC size parsed from footer")
			}

			toc, err := fetchRange(ctx, resolver, ref, desc, tocOff, tocSize)
			if err != nil {
				log.Warnf("error fetching TOC: %s", err.Error())
				return nil, fmt.Errorf("failed fetching TOC: %w", err)
			}

			jsonBytes, err := uncompressTOC(toc)
			if err != nil {
				return nil, fmt.Errorf("failed uncompressing zstd:chunked TOC")
			}
			log.Debugf("    Uncompressed TOC of %d bytes", len(jsonBytes))
		}
		// This ensures the blob will still be downloaded by containerd's default fetchers.
		return nil, nil
	}, nil
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

func fetchRange(ctx context.Context, resolver remotes.Resolver, ref string, desc ocispec.Descriptor, offset int64, length int64) ([]byte, error) {
	fetcher, err := resolver.Fetcher(ctx, ref)
	if err != nil {
		return nil, err
	}

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
	defer rc.Close()
	return io.ReadAll(rc)
}

func uncompressTOC(tocBytes []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(bytes.NewReader(tocBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer decoder.Close()

	uncompressedTOC, err := io.ReadAll(decoder)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress TOC: %w", err)
	}

	return uncompressedTOC, nil
}
