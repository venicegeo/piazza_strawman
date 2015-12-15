package com.radiantblue.normalizer

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._

import scala.collection.JavaConverters._

object InspectZippedShapefile {
  val logger = org.slf4j.LoggerFactory.getLogger(InspectZippedShapefile.getClass)

  def thread(f: => Any): java.lang.Thread =
    new java.lang.Thread {
      override def run(): Unit = { f }
      start()
    }

  val parseUpload = fromJsonBytes[Upload]
  val formatMetadata = toJsonBytes[GeoMetadata]

  def main(args: Array[String]): Unit = {
    val producer = com.radiantblue.piazza.kafka.Kafka.producer[String, Array[Byte]]()
    val consumer = com.radiantblue.piazza.kafka.Kafka.consumer("inspect-zipped-shapefile")
    val streams = consumer.createMessageStreamsByFilter(Whitelist("uploads"))
    val threads = streams.map { stream =>
      thread {
        stream.foreach { message =>
          try {
            val upload = parseUpload(message.message)
            logger.info("Upload {}", upload)
            val path = (new com.radiantblue.deployer.FileSystemDatasetStorage()).lookup(upload.locator)
            logger.info("path {}", path)
            val result = InspectZippedShapefile.inspect(upload.locator, path.toFile)

            result match {
              case Left(ex) =>
                logger.error("Failed to handle shapefile", ex)
              case Right(metadata) =>
                producer.send(new KeyedMessage("metadata", formatMetadata(metadata)))
                logger.info("Emitted {}", metadata)
            }
          } catch {
            case scala.util.control.NonFatal(ex) =>
              logger.error("Failed to extract shapefile metadata", ex)
          }
        }
      }
    }
    threads.foreach { _.join() }
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
      nativeFormat="zipped-shapefile")
  }

  private def toBoundingBox(e: org.opengis.geometry.Envelope): Bounds =
    Bounds(
      minX=e.getMinimum(0),
      maxX=e.getMaximum(0),
      minY=e.getMinimum(1),
      maxY=e.getMaximum(1))

  def inspect(locator: String, file: java.io.File): Either[Throwable, GeoMetadata] = {
    try {
      val zip = new java.util.zip.ZipFile(file)
      val workDir = 
        try {
          val names = 
            (for (e <- zip.entries.asScala) yield e.getName).to[Vector]
          val basenames = names.map(_.replaceFirst("\\..*$", ""))
          require(basenames.distinct.size == 1)
          require(names.exists(_.toLowerCase endsWith ".shp"))

          val workDir = java.nio.file.Files.createTempDirectory("unpack-zipped-shapefile")
          for (e <- zip.entries.asScala) {
            val path = workDir.resolve(e.getName)
            val stream = zip.getInputStream(e)
            try java.nio.file.Files.copy(stream, path)
            finally stream.close()
          }
          workDir
        } finally zip.close()

      val params = Map[String, java.io.Serializable]("url" -> workDir.toUri.toString)
      val storeFactory = new org.geotools.data.shapefile.ShapefileDataStoreFactory
      val store = storeFactory.createDataStore(params.asJava)
      try {
        val source = store.getFeatureSource(store.getNames.asScala.head)
        Right(geoMetadata(locator, source.getSchema.getCoordinateReferenceSystem, source.getBounds))
      } finally store.dispose()
    } catch {
      case scala.util.control.NonFatal(ex) =>
        Left(ex)
    }
  }
}
