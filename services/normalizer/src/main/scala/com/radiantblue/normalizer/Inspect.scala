package com.radiantblue.normalizer

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._

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
    val producer = com.radiantblue.piazza.kafka.Kafka.producer[String, Array[Byte]]()
    val consumer = com.radiantblue.piazza.kafka.Kafka.consumer("inspect")
    val streams = consumer.createMessageStreamsByFilter(Whitelist("uploads"))
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

    threads.foreach { _.join() }
  }
}
