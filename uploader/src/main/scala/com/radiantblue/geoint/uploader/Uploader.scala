package com.radiantblue.geoint.uploader

import com.radiantblue.geoint.Messages

object Uploader {
  def main(args: Array[String]): Unit = {
    val Array(filename) = args
    val upload = Messages.Upload.newBuilder.setName(filename).build()
    val props = new java.util.Properties()
    props.put("zk.connect", "127.0.0.1:2181")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("metadata.broker.list", "127.0.0.1:9092")
    val config = new kafka.producer.ProducerConfig(props)
    val producer = new kafka.javaapi.producer.Producer[String, Array[Byte]](config)
    try {
      val message = new kafka.producer.KeyedMessage[String, Array[Byte]]("uploads", upload.toByteArray)
      producer.send(message)
    } finally {
      producer.close
    }
  }
}
