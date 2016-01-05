package com.radiantblue.piazza.uploader

import com.radiantblue.piazza.Upload
import com.radiantblue.piazza.JsonProtocol._

object Uploader {
  val format = toJsonBytes[Upload]

  def main(args: Array[String]): Unit = {
    val Array(filename) = args
    val upload = Upload(name=filename, locator="/tmp/foo", jobId=???)
    val producer = com.radiantblue.piazza.kafka.Kafka.producer[String, Array[Byte]]()
    try {
      val message = new kafka.producer.KeyedMessage[String, Array[Byte]]("uploads", format(upload))
      producer.send(message)
    } finally {
      producer.close
    }
  }
}
