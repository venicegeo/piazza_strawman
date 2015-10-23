#!/bin/sh

set -ex

cp services/uploader/target/universal/uploader-0.1-SNAPSHOT.tgz ansible/roles/uploader/files/
cp services/ogcproxy/target/universal/ogcproxy-0.1-SNAPSHOT.tgz ansible/roles/ogcproxy/files/
cp services/deployer/target/universal/deployer-0.1-SNAPSHOT.tgz ansible/roles/deployer/files/
cp services/normalizer/target/scala-2.10/normalizer-assembly-0.1-SNAPSHOT.jar ansible/roles/normalizer/files/
