set -ex

pushd /opt/kafka
for topic in uploads metadata lease-requests lease-grants simplify;
do
    bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic "$topic" --partitions 20 --replication-factor 1
done
touch deployed_piazza_queues
popd
