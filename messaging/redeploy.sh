set -ex

pushd /opt/storm
bin/storm deactivate PersistMetadata
bin/storm kill PersistMetadata
sleep 30
bin/storm jar ~/sync/normalizer-assembly-0.1-SNAPSHOT.jar com.radiantblue.normalizer.PersistMetadata
bin/storm activate PersistMetadata
popd
