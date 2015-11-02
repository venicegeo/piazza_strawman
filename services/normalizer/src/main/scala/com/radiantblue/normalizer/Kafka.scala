package com.radiantblue.normalizer


/**
 * Common setup code for Kafka/Storm connectors.
 */
object Kafka {
  import com.radiantblue.piazza.kafka.{ Kafka => Config }
  /**
   * Create a Spout that will read and emit all messages from a Kafka topic
   * using the provided Scheme to produce Storm tuples.
   */
  def spoutForTopic(topic: String, scheme: backtype.storm.spout.Scheme): storm.kafka.KafkaSpout = {
    val hosts = new storm.kafka.ZkHosts(Config.zookeepers.mkString(","))
    val spoutConfig = new storm.kafka.SpoutConfig(hosts, topic, "", topic)
    spoutConfig.scheme = new backtype.storm.spout.SchemeAsMultiScheme(scheme)
    new storm.kafka.KafkaSpout(spoutConfig)
  }

  def boltForTopic[K,V](topic: String, mapper: storm.kafka.bolt.mapper.TupleToKafkaMapper[K, V]): storm.kafka.bolt.KafkaBolt[K, V] =
    (new storm.kafka.bolt.KafkaBolt()
      .withTopicSelector(new storm.kafka.bolt.selector.DefaultTopicSelector(topic))
      .withTupleToKafkaMapper(mapper))

  def topologyConfig: backtype.storm.Config = {
    val conf = new backtype.storm.Config
    val props = new java.util.Properties
    props.put("metadata.broker.list", Config.brokers.mkString(","))
    conf.put(storm.kafka.bolt.KafkaBolt.KAFKA_BROKER_PROPERTIES, props)
    conf
  }
}
