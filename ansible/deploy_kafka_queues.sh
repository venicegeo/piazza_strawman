set -ex

pushd /opt/kafka
for topic in uploads metadata lease-requests lease-grants simplify-requests;
do
    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic "$topic" --partitions 1 --replication-factor 1
done
touch deployed_piazza_queues
popd
