package com.radiantblue.normalizer

import scala.collection.JavaConverters._
import kafka.consumer.Whitelist
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._
import com.radiantblue.piazza.postgres._

object Persist {
  val logger = org.slf4j.LoggerFactory.getLogger(Persist.getClass)

  def thread(f: => Any): java.lang.Thread =
    new java.lang.Thread {
      override def run(): Unit = { f }
      start()
    }

  def main(args: Array[String]): Unit = {
    val parseMetadata = fromJsonBytes[Metadata]
    val parseGeoMetadata = fromJsonBytes[GeoMetadata]
    val consumer = com.radiantblue.piazza.kafka.Kafka.consumer("metadata-persisters")
    val streams = consumer.createMessageStreamsByFilter(Whitelist("metadata"))
    val conn = Postgres("piazza.metadata.postgres").connect()
    val threads = streams.map { stream =>
      thread {
        stream.foreach { message =>
          try {
            val metadata = try {
              Left(parseMetadata(message.message))
            } catch {
              case scala.util.control.NonFatal(_) =>
                Right(parseGeoMetadata(message.message))
            }

            metadata match {
              case Left(metadata) => conn.insertMetadata(metadata)
              case Right(geometadata) => conn.insertGeoMetadata(geometadata)
            }
          } catch {
            case scala.util.control.NonFatal(ex) =>
              logger.error("Metadata persistence failed", ex)
          }
        }
      }
    }

    threads.foreach { _.join() }
  }
}
