package com.radiantblue.normalizer

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._
import scala.collection.JavaConverters._

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object InspectGeoTiff {
  val logger = org.slf4j.LoggerFactory.getLogger(InspectGeoTiff.getClass)

  def thread(f: => Any): java.lang.Thread =
    new java.lang.Thread {
      override def run(): Unit = { f }
      start()
    }

  val parseUpload = fromJsonBytes[Upload]
  val formatMetadata = toJsonBytes[GeoMetadata]

  def main(args: Array[String]): Unit = {
    val producer = com.radiantblue.piazza.kafka.Kafka.newProducer[String, Array[Byte]]()
    val consumer = com.radiantblue.piazza.kafka.Kafka.newConsumer[String, Array[Byte]]("inspect-geotiff")
    consumer.subscribe(java.util.Arrays.asList("uploads"))

    while (true) {
      val records = consumer.poll(1000)
      for(record <- records) {
        val message = record.value()
        try {
          val upload = parseUpload(message/*.message*/)
          logger.debug("Upload {}", upload)
          val path = (new com.radiantblue.deployer.FileSystemDatasetStorage()).lookup(upload.locator)
          logger.debug("path {}", path)
          val result = InspectGeoTiff.inspect(upload.locator, path.toFile)

          result.foreach { r =>
            val keyedMessage = new ProducerRecord[String, Array[Byte]]("metadata", formatMetadata(r));
            producer.send(keyedMessage)
            /*producer.send(new KeyedMessage("metadata", formatMetadata(r)))*/
            logger.debug("Emitted {}", r)
          }
        } catch {
          case scala.util.control.NonFatal(ex) =>
            logger.error("Failed to extract geotiff metadata", ex)
        }
      }
    }

    /*val streams = consumer.createMessageStreamsByFilter(Whitelist("uploads"))
    val threads = streams.map { stream =>
      thread {
        stream.foreach { message =>
          try {
            val upload = parseUpload(message.message)
            logger.debug("Upload {}", upload)
            val path = (new com.radiantblue.deployer.FileSystemDatasetStorage()).lookup(upload.locator)
            logger.debug("path {}", path)
            val result = InspectGeoTiff.inspect(upload.locator, path.toFile)

            result.foreach { r =>
              producer.send(new KeyedMessage("metadata", formatMetadata(r)))
              logger.debug("Emitted {}", r)
            }
          } catch {
            case scala.util.control.NonFatal(ex) =>
              logger.error("Failed to extract geotiff metadata", ex)
          }
        }
      }
    }
    threads.foreach { _.join() }*/


  }

  private def geoMetadata(
    locator: String,
    crs: org.opengis.referencing.crs.CoordinateReferenceSystem,
    envelope: org.opengis.geometry.BoundingBox
  ): GeoMetadata = {
    import org.geotools.referencing.CRS
    val srid = CRS.lookupIdentifier(crs, true)
    val latLonEnvelope = {
      val wgs84 = CRS.decode("EPSG:4326")
      val tx = CRS.findMathTransform(crs, wgs84)
      CRS.transform(tx, envelope)
    }

    GeoMetadata(
      locator=locator,
      crsCode=srid,
      nativeBoundingBox=toBoundingBox(envelope),
      latitudeLongitudeBoundingBox=toBoundingBox(latLonEnvelope),
      nativeFormat="geotiff")
  }

  private def toBoundingBox(e: org.opengis.geometry.Envelope): Bounds =
    Bounds(
      minX=e.getMinimum(0),
      maxX=e.getMaximum(0),
      minY=e.getMinimum(1),
      maxY=e.getMaximum(1))

  private def tryFindFormat(x: AnyRef): Option[org.geotools.coverage.grid.io.AbstractGridFormat] = 
    try
      Some(org.geotools.coverage.grid.io.GridFormatFinder.findFormat(x))
        .filterNot(_.isInstanceOf[org.geotools.coverage.grid.io.UnknownFormat])
    catch {
      case _: java.lang.UnsupportedOperationException => None
    }

  def inspect(locator: String, file: java.io.File): Option[GeoMetadata] = {
    for (format <- tryFindFormat(file)) yield {
      val reader = format.getReader(file)
      try {
        val coverage = reader.read(null)
        try {
          val envelope = coverage.getEnvelope2D
          val crs = coverage.getCoordinateReferenceSystem
          geoMetadata(locator, crs, envelope)
        } finally {
          coverage.dispose(true)
        }
      } finally {
        reader.dispose()
      }
    }
  }
}
