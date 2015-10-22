#!/bin/sh

set -ex

cp ../geoint_messaging/uploader/target/universal/uploader-0.1-SNAPSHOT.tgz ansible/roles/uploader/files/
cp ../geoint_messaging/ogcproxy/target/universal/ogcproxy-0.1-SNAPSHOT.tgz ansible/roles/ogcproxy/files/
cp ../geoint_messaging/deployer/target/universal/deployer-0.1-SNAPSHOT.tgz ansible/roles/deployer/files/
cp ../geoint_messaging/normalizer/target/scala-2.10/normalizer-assembly-0.1-SNAPSHOT.jar ansible/roles/normalizer/files/
