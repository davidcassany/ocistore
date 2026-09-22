# OCIStore

A daemonless OCI image storage library and CLI built on the [containerd v2](https://github.com/containerd/containerd)
stack — no containerd daemon required.

Born as a playground within the [Elemental Toolkit](https://github.com/rancher/elemental-toolkit) project, OCIStore
explores OCI image handling and, in particular, efficient extraction of `zstd:chunked` images into a single flattened
filesystem using delta fetches. It relies on Podman's `zstd:chunked` format for delta extraction.

## Build

```bash
make build
```

## Usage

```
ocistore [command] [flags]

Commands:
  commit         Commit an active snapshot as a new image
  delete         Delete an image
  extract        Pull and extract an image's flattened root tree to a directory
  import         Import an OCI archive
  list           List all images
  list-snapshots List all available snapshots
  mount          Mount an image to a target mountpoint
  pull           Pull a remote image into the local containerd store
  umount         Unmount a mountpoint
  unpack         Unpack an image

Global Flags:
      --debug             Enable debug logging
      --loglevel string   Set log output level
      --root string       Path for the local containerd store (default "/tmp/ocistore")
```

## zstd:chunked Extraction with Delta Fetches

The `extract` command pulls a remote image and extracts its flattened root filesystem:

```bash
ocistore extract IMAGE_REF DESTINATION [flags]

Flags:
      --delta           Only fetch files not present in the local cache (default true)
      --filedb string   Path for the local files database (default "/tmp/ocistore/files.db")
      --skip-tls        Skip TLS verification
```

When extracting a `zstd:chunked` image, OCIStore reads the Table of Contents from the compressed stream and compares
it against a local file database built from previous extractions. Only missing or changed files are downloaded —
files already cached are reused locally. This results in minimal network transfers when updating between image versions
or builds of the same image.

To build a `zstd:chunked` image, push with:

```bash
podman push --compression-format zstd:chunked <image-ref>
```

### Test Image

A test image (`test/multilayer:latest`) is published at [openSUSE Build Service](https://build.opensuse.org/package/show/home:dcassany:containers/extractiontest-image).
It is an image which adds a couple of additional layers over a minimal general purpose base to test some extraction cases:

* File type transitions across layers: directory -> file, directory -> symlink and file- > directory
* Attribute mutation
* Extraction of devices
* Hardlinks management when the source is whiteout
* Whiteouts and opaques
* Download duplicated files from the same image only once regardless of being present in the same layer or a different one

To extract the image and test delta download:

```bash
# Extract the base image - it will populate the cached files database
sudo ocistore --debug extract \
  registry.opensuse.org/home/dcassany/containers/zstd-chunked/test/base:latest \
  ./extractions/base

# Extract test/multilayer:latest — built on top of the previous one and observe the
# delta download only the files from top layers will be downloaded
sudo ocistore --debug extract \
  registry.opensuse.org/home/dcassany/containers/zstd-chunked/test/multilayer:latest \
  ./extractions/partially_cached

# Extract test/multilayer:latest again to verify it only fetches the ToC and no file
# is identified as a miss
sudo ocistore --debug extract \
  registry.opensuse.org/home/dcassany/containers/zstd-chunked/test/multilayer:latest \
  ./extractions/fully_cached
```

#### Verify extracted images

To verify the extracted image `skopeo`, `umoci` and `mtree` can be used to confirm the extraction is valid.

```bash
# extract the remote image filesystem using skopeo and umoci
skopeo copy \
  docker://registry.opensuse.org/home/dcassany/containers/zstd-chunked/test/multilayer:latest \
  oci:.extractions/multilayer_oci_image:latest

sudo umoci unpack --image ./extractions/multilayer_oci_image:latest ./extractions/reference

# Create an mtree report of the extracted image to validate extractions done with the
# ocistore utility
sudo mtree -c -p ./extractions/reference/rootfs \
  -k uid,gid,mode,size,type,link,sha256 > ./extractions/validation.mtree

# Validate partial and fully cached extractions
sudo mtree -f ./extractions/validation.mtree -p ./extractions/partially_cached
sudo mtree -f ./extractions/validation.mtree -p ./extractions/fully_cached
```

## License

See [LICENSE](LICENSE).
