#!/bin/sh

# First-time setup before Piazza Ansible scripts can run. This makes some
# resources available that aren't suitable for inclusion in the Git repository,
# like large third-party files and security keys.

set -ex # be verbose and fail at the first error

# Use which to fail if some uncommon tools that we use are not present
which vagrant
which ansible
which ssh-keygen
which wget
which java

# Create an SSH key just for this deployment
ssh-keygen -N '' -f geoserver-files
mkdir ansible/roles/deployer/files
mv geoserver-files ansible/roles/deployer/files/
mv geoserver-files.pub ansible/roles/geoserver_file_receiver/files/

# Build Scala services
(cd services/ && sbt/bin/sbt universal:packageZipTarball)

# Copy built artifacts
. copy_builds.sh

# Fetch software from offline that yum won't fetch for us
. third-party-downloads.sh

echo Finished successfully - ready to run.
