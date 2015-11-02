set -ex

pushd /opt/storm
bin/storm jar /opt/normalizer/normalizer-assembly-0.1-SNAPSHOT.jar com.radiantblue.normalizer.ExtractMetadata
bin/storm jar /opt/normalizer/normalizer-assembly-0.1-SNAPSHOT.jar com.radiantblue.normalizer.ExtractGeoTiffMetadata
bin/storm jar /opt/normalizer/normalizer-assembly-0.1-SNAPSHOT.jar com.radiantblue.normalizer.PersistMetadata
bin/storm activate ExtractMetadata
bin/storm activate ExtractGeoTiffMetadata
bin/storm activate PersistMetadata
touch deployed_piazza_topologies
popd
