package com.radiantblue.piazza.kafka

import scala.collection.JavaConverters._

object Kafka {
  lazy val config = com.typesafe.config.ConfigFactory.load()

  def zookeepers: Vector[String] = 
    config.getStringList("piazza.kafka.zookeepers").asScala.to[Vector]

  def brokers: Vector[String] =
    config.getStringList("piazza.kafka.brokers").asScala.to[Vector]

  def producer[K, T](extraOpts: (String, String)*): kafka.javaapi.producer.Producer[K, T] = {
    val props = new java.util.Properties
    props.put("zk.connect", zookeepers.mkString(","))
    props.put("metadata.broker.list", brokers.mkString(","))
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    extraOpts.foreach { case (k, v) => props.put(k, v) }
    val producerConfig = new kafka.producer.ProducerConfig(props)
    new kafka.javaapi.producer.Producer[K, T](producerConfig)
  }
}
