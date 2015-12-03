package com.radiantblue.piazza.uploader

import com.radiantblue.piazza.Upload
import com.radiantblue.piazza.JsonProtocol._

object Uploader {
  val format = toJsonBytes[Upload]

  def main(args: Array[String]): Unit = {
    val Array(filename) = args
    val upload = Upload(name=filename, locator="/tmp/foo")
    val props = new java.util.Properties()
    props.put("zk.connect", "127.0.0.1:2181")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("metadata.broker.list", "127.0.0.1:9092")
    val config = new kafka.producer.ProducerConfig(props)
    val producer = new kafka.javaapi.producer.Producer[String, Array[Byte]](config)
    try {
      val message = new kafka.producer.KeyedMessage[String, Array[Byte]]("uploads", format(upload))
      producer.send(message)
    } finally {
      producer.close
    }
  }
}
