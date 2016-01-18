package com.radiantblue.normalizer

import scala.collection.JavaConverters._
import kafka.consumer.Whitelist
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._
import com.radiantblue.piazza.postgres._

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import java.io._

object Persist {
  val logger = org.slf4j.LoggerFactory.getLogger(Persist.getClass)

  def thread(f: => Any): java.lang.Thread =
    new java.lang.Thread {
      override def run(): Unit = { f }
      start()
    }

  def main(args: Array[String]): Unit = {
    val fw = new PrintWriter(new File("persistmetadata" ))
    fw.write("initializing persist")
    fw.flush()    
    val parseMetadata = fromJsonBytes[Metadata]
    val parseGeoMetadata = fromJsonBytes[GeoMetadata]
    val consumer = com.radiantblue.piazza.kafka.Kafka.newConsumer[String, Array[Byte]]("metadata-persisters")
    consumer.subscribe(java.util.Arrays.asList("metadata"))
    /*val streams = consumer.createMessageStreamsByFilter(Whitelist("metadata"))*/
    val conn = Postgres("piazza.metadata.postgres").connect()
    while (true) {
      val records = consumer.poll(100)
      for(record <- records) {   
        fw.write("consuming message on persist")
        fw.flush()     
        try {
          val message = record.value()
          val metadata = try {
            Left(parseMetadata(message/*.message*/))
          } catch {
            case scala.util.control.NonFatal(_) =>
              Right(parseGeoMetadata(message/*.message*/))
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
    fw.close()
    /*
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

    threads.foreach { _.join() }*/
  }
}
