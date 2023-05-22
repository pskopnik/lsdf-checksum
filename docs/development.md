# Development

## End-to-End Testing

There are very few unit tests throughout the entire codebase.
The only reliable method for testing the lsdf-checksum system is through end-to-end tests.

A tool for performing automated tests is available in `tools/e2e-tester/`.
After any significant changes, the tool should be executed with multiple steps.
Steps should change many to extremely few files to test edge conditions in database access.

## Release Procedure

 * Plan version name.

   Follow [semantic versioning](https://semver.org/).

 * Build main executables.

   ```shell
   $ go build ./cmd/lsdf-checksum-master
   $ go build ./cmd/lsdf-checksum-worker
   ```

 * Create release tar.

   ```shell
   $ mkdir lsdf-checksum-$VERSION
   $ cp lsdf-checksum-master lsdf-checksum-worker lsdf-checksum-$VERSION
   $ tar -czf lsdf-checksum-$VERSION.tar.gz lsdf-checksum-$VERSION
   ```

 * Make sure end-to-end tests are passing!

 * RPMs are used to package and deploy the tool - at least at KIT.
   The spec file is in `build/`.

   * Update `upstream_version` in the spec file, add a changelog section.
     The changelog must have the version in the proper EVR format (see conversion command in for `Version:`).

   * Build RPMs for EL7 and EL8.
     Follow the procedure from [httpd-webdav](https://codebase.helmholtz.cloud/kit-scc-sdm/onlinestorage/httpd-webdav) for building RPMs in Docker.

 * Tag commit and push tag.
