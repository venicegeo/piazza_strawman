package com.radiantblue.normalizer

import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._

import scala.collection.JavaConverters._

object InspectZippedShapefile {
  val bolt: backtype.storm.topology.IRichBolt = new backtype.storm.topology.base.BaseRichBolt {
    val logger = org.slf4j.LoggerFactory.getLogger(InspectZippedShapefile.getClass)
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      try {
        val format = toJsonBytes[GeoMetadata]
        val upload = tuple.getValue(0).asInstanceOf[Upload]
        logger.info("Upload {}", upload)
        val path = (new com.radiantblue.deployer.FileSystemDatasetStorage()).lookup(upload.locator)
        logger.info("path {}", path)
        val result = InspectZippedShapefile.inspect(upload.locator, path.toFile)

        result match {
          case Left(ex) => 
            logger.error("Failed to handle shapefile", ex)
          case Right(metadata) =>
            _collector.emit(tuple, java.util.Arrays.asList[AnyRef](format(metadata)))
            logger.info("emitted")
        }

        _collector.ack(tuple)
      } catch {
        case scala.util.control.NonFatal(ex) => 
          logger.error("Failed to handle shapefile", ex)
          _collector.fail(tuple)
      }
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    } 

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declare(new backtype.storm.tuple.Fields("message"))
    }
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

  def main(args: Array[String]) {
    println(inspect(args(0), new java.io.File(args(0))))
  }
}
