package com.radiantblue.normalizer

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import java.io._

object Inspect {
  val logger = org.slf4j.LoggerFactory.getLogger(Inspect.getClass)

  def thread(f: => Any): java.lang.Thread =
    new java.lang.Thread {
      override def run(): Unit = { f }
      start()
    }

  val parseUpload = fromJsonBytes[Upload]
  val formatMetadata = toJsonBytes[Metadata]

  def main(args: Array[String]): Unit = {
    val producer = com.radiantblue.piazza.kafka.Kafka.newProducer[String, Array[Byte]]()
    val consumer = com.radiantblue.piazza.kafka.Kafka.newConsumer[String, Array[Byte]]("inspect")
    consumer.subscribe(java.util.Arrays.asList("uploads"))
    val fw = new PrintWriter(new File("inspect" ))
    fw.write("listening for message")
    fw.flush()
    /*val streams = consumer.createMessageStreamsByFilter(Whitelist("uploads"))*/
    while (true) {
      val records = consumer.poll(1000)
      for(record <- records) {    
        try {
          val message = record.value()
          val upload = parseUpload(message/*.message*/)

          logger.debug("Upload {}", upload)
          val path = (new com.radiantblue.deployer.FileSystemDatasetStorage()).lookup(upload.locator)
          logger.debug("path {}", path)
          val size = java.nio.file.Files.getAttribute(path, "size").asInstanceOf[java.lang.Long]
          logger.debug("size {}", size)
          val checksum = {
            val stream = new java.io.FileInputStream(path.toFile)
            try {
              val buff = Array.ofDim[Byte](16384)
              var amountRead = 0
              val digest = java.security.MessageDigest.getInstance("MD5")
              while ({
                amountRead = stream.read(buff)
                amountRead >= 0
              }) {
                digest.update(buff, 0, amountRead)
              }
              digest.digest()
            } finally {
              stream.close()
            }
          }
          logger.debug("Checksum {}", checksum.map(b => f"$b%02X").mkString)

          val metadata = Metadata(
            name=upload.name,
            locator=upload.locator,
            jobId=upload.jobId,
            checksum=checksum.to[Vector],
            size=size)
          val forwardedMessage = new ProducerRecord[String, Array[Byte]]("metadata", formatMetadata(metadata))
          /*producer.send(new KeyedMessage("metadata", formatMetadata(metadata)))*/
          producer.send(forwardedMessage)
          logger.debug(s"Emitted $metadata")
        } catch {
          case scala.util.control.NonFatal(ex) => logger.error("Error extracting metadata", ex)
        }
      }    
    }

/*
    val threads = streams.map { stream =>
      thread {
        stream.foreach { message =>
          try {
            val upload = parseUpload(message.message)

            logger.debug("Upload {}", upload)
            val path = (new com.radiantblue.deployer.FileSystemDatasetStorage()).lookup(upload.locator)
            logger.debug("path {}", path)
            val size = java.nio.file.Files.getAttribute(path, "size").asInstanceOf[java.lang.Long]
            logger.debug("size {}", size)
            val checksum = {
              val stream = new java.io.FileInputStream(path.toFile)
              try {
                val buff = Array.ofDim[Byte](16384)
                var amountRead = 0
                val digest = java.security.MessageDigest.getInstance("MD5")
                while ({
                  amountRead = stream.read(buff)
                  amountRead >= 0
                }) {
                  digest.update(buff, 0, amountRead)
                }
                digest.digest()
              } finally {
                stream.close()
              }
            }
            logger.debug("Checksum {}", checksum.map(b => f"$b%02X").mkString)

            val metadata = Metadata(
              name=upload.name,
              locator=upload.locator,
              jobId=upload.jobId,
              checksum=checksum.to[Vector],
              size=size)
            producer.send(new KeyedMessage("metadata", formatMetadata(metadata)))
            logger.debug(s"Emitted $metadata")
          } catch {
            case scala.util.control.NonFatal(ex) => logger.error("Error extracting metadata", ex)
          }
        }
      }
    }

    threads.foreach { _.join() }*/
  }
}
