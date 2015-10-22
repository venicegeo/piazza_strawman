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

ssh-keygen -N '' -f geoserver-files
mv geoserver-files ansible/roles/deployer/files/
mv geoserver-files.pub ansible/roles/geoserver_file_receiver/files/

. copy_builds.sh

. third-party-downloads.sh

echo Finished successfully - ready to run.
