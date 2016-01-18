package com.radiantblue.piazza.kafka

import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._

object Kafka {
  lazy val config = com.typesafe.config.ConfigFactory.load()

  def zookeepers: Vector[String] =
    config.getStringList("piazza.kafka.zookeepers").asScala.to[Vector]

  def brokers: Vector[String] =
    config.getStringList("piazza.kafka.brokers").asScala.to[Vector]

  def producer[K, T](extraOpts: (String, String)*): kafka.producer.Producer[K, T] = {
    val props = new java.util.Properties
    /*props.put("zk.connect", zookeepers.mkString(","))*/
    props.put("metadata.broker.list", brokers.mkString(","))
    props.put("bootstrap.servers", "kafka.dev:9092");
    props.put("backoff.increment.ms", "1000")
    props.put("zk.read.num.retries", "3")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    extraOpts.foreach { case (k, v) => props.put(k, v) }
    val producerConfig = new kafka.producer.ProducerConfig(props)
    new kafka.producer.Producer[K, T](producerConfig)
  }

  def newProducer[K, T](extraOpts: (String, String)*): KafkaProducer[K, T] = {
    val props = new java.util.Properties
    /*props.put("zk.connect", zookeepers.mkString(","))*/
    props.put("bootstrap.servers", "kafka.dev:9092");
    props.put("acks", "all");
    props.put("retries", "0");
    props.put("batch.size", "16384");
    props.put("linger.ms", "1");
    props.put("buffer.memory", "33554432");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    new KafkaProducer[K, T](props)
  }

  def consumer(groupId: String, extraOpts: (String, String)*): kafka.consumer.ConsumerConnector = {
    val props = new java.util.Properties
    props.put("group.id", groupId)
    props.put("zookeeper.connect", zookeepers.mkString(","))
    extraOpts.foreach { case (k, v) => props.put(k, v) }
    kafka.consumer.Consumer.create(new kafka.consumer.ConsumerConfig(props))
  }

  def newConsumer[K, T](groupId: String, extraOpts: (String, String)*): KafkaConsumer[K, T] = {
    val props = new java.util.Properties
    props.put("bootstrap.servers", "kafka.dev:9092");
    props.put("group.id", groupId)
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");    
    new KafkaConsumer[K, T](props)
  }
}
