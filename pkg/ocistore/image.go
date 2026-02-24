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
	"context"
	"errors"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/opencontainers/image-spec/identity"
)

func (c *OCIStore) Get(ref string) (client.Image, error) {
	if !c.IsInitiated() {
		return nil, errors.New(missInitErrMsg)
	}

	img, err := c.cli.GetImage(c.ctx, ref)
	if err != nil {
		return nil, err
	}

	return img, nil
}

func (c *OCIStore) List(filters ...string) ([]client.Image, error) {
	if !c.IsInitiated() {
		return nil, errors.New(missInitErrMsg)
	}

	images, err := c.cli.ListImages(c.ctx, filters...)
	if err != nil {
		return nil, err
	}

	return images, nil
}

func (c *OCIStore) Delete(name string, opts ...images.DeleteOpt) (retErr error) {
	if !c.IsInitiated() {
		return errors.New(missInitErrMsg)
	}

	ctx, done, err := c.cli.WithLease(c.ctx, leases.WithRandomID(), leases.WithExpiration(1*time.Hour))
	if err != nil {
		c.log.Errorf("failed to create lease to delete image: %v", err)
		return err
	}
	defer func() {
		err = done(ctx)
		if err != nil && retErr == nil {
			c.log.Warnf("could not remove lease on delete image operation")
		}
	}()

	err = c.delete(ctx, name, opts...)
	if err != nil {
		c.log.Errorf("failed deleting image '%s': %v", name, err)
		return err
	}

	c.log.Infof("Successfully deleted image '%s'", name)
	return nil
}

func (c *OCIStore) Update(img images.Image, fieldpaths ...string) (_ client.Image, retErr error) {
	if !c.IsInitiated() {
		return nil, errors.New(missInitErrMsg)
	}

	ctx, done, err := c.cli.WithLease(c.ctx, leases.WithRandomID(), leases.WithExpiration(1*time.Hour))
	if err != nil {
		c.log.Errorf("failed to create lease to update image: %v", err)
		return nil, err
	}
	defer func() {
		err = done(ctx)
		if err != nil && retErr == nil {
			c.log.Warnf("could not remove lease on update image operation")
		}
	}()

	i, err := c.cli.ImageService().Update(ctx, img, fieldpaths...)
	if err != nil {
		return nil, err
	}

	return client.NewImage(c.cli, i), nil
}

func (c *OCIStore) Create(img images.Image) (_ client.Image, retErr error) {
	if !c.IsInitiated() {
		return nil, errors.New(missInitErrMsg)
	}

	ctx, done, err := c.cli.WithLease(c.ctx, leases.WithRandomID(), leases.WithExpiration(1*time.Hour))
	if err != nil {
		c.log.Errorf("failed to create lease to create image: %v", err)
		return nil, err
	}
	defer func() {
		err = done(ctx)
		if err != nil && retErr == nil {
			c.log.Warnf("could not remove lease on create image operation")
		}
	}()

	i, err := c.cli.ImageService().Create(ctx, img)
	if err != nil {
		return nil, err
	}

	return client.NewImage(c.cli, i), nil
}

func (c *OCIStore) delete(ctx context.Context, name string, opts ...images.DeleteOpt) error {
	img, err := c.cli.GetImage(ctx, name)
	if err != nil {
		return err
	}
	if ok, err := img.IsUnpacked(ctx, c.driver); ok {
		diffIDs, err := img.RootFS(ctx)
		if err != nil {
			return err
		}
		chainID := identity.ChainID(diffIDs).String()
		sn := c.cli.SnapshotService(c.driver)
		err = c.removeSnapshotsChain(ctx, sn, chainID, -1)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	return c.cli.ImageService().Delete(ctx, name, opts...)
}
