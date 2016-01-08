package com.radiantblue.piazza.kafka

import scala.collection.JavaConverters._

object Kafka {
  lazy val config = com.typesafe.config.ConfigFactory.load()

  def zookeepers: Vector[String] =
    config.getStringList("piazza.kafka.zookeepers").asScala.to[Vector]

  def brokers: Vector[String] =
    config.getStringList("piazza.kafka.brokers").asScala.to[Vector]

  def producer[K, T](extraOpts: (String, String)*): kafka.producer.Producer[K, T] = {
    val props = new java.util.Properties
    props.put("zk.connect", zookeepers.mkString(","))
    props.put("metadata.broker.list", brokers.mkString(","))
    props.put("retry.backoff.ms", "1000")
    props.put("retries", "10")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    extraOpts.foreach { case (k, v) => props.put(k, v) }
    val producerConfig = new kafka.producer.ProducerConfig(props)
    new kafka.producer.Producer[K, T](producerConfig)
  }

  def consumer(groupId: String, extraOpts: (String, String)*): kafka.consumer.ConsumerConnector = {
    val props = new java.util.Properties
    props.put("group.id", groupId)
    props.put("zookeeper.connect", zookeepers.mkString(","))
    extraOpts.foreach { case (k, v) => props.put(k, v) }
    kafka.consumer.Consumer.create(new kafka.consumer.ConsumerConfig(props))
  }
}
