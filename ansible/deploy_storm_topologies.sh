set -ex

pushd /opt/storm
bin/storm jar /opt/normalizer/normalizer-assembly-0.1-SNAPSHOT.jar com.radiantblue.normalizer.MetadataTopology
# bin/storm jar /opt/normalizer/normalizer-assembly-0.1-SNAPSHOT.jar com.radiantblue.normalizer.PersistTopology
bin/storm activate Metadata
# bin/storm activate Persist
touch deployed_piazza_topologies
popd
