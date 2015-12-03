package com.radiantblue.normalizer

import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._
import scala.collection.JavaConverters._

object InspectGeoTiff {
  val bolt: backtype.storm.topology.IRichBolt = new backtype.storm.topology.base.BaseRichBolt {
    val logger = org.slf4j.LoggerFactory.getLogger(InspectGeoTiff.getClass)
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val format = toJsonBytes[GeoMetadata]
      val upload = tuple.getValue(0).asInstanceOf[Upload]
      logger.info("Upload {}", upload)
      val path = (new com.radiantblue.deployer.FileSystemDatasetStorage()).lookup(upload.locator)
      logger.info("path {}", path)
      val result = InspectGeoTiff.inspect(upload.locator, path.toFile)

      result.foreach { r =>
        _collector.emit(tuple, java.util.Arrays.asList[AnyRef](format(r)))
        logger.info("emitted")
      }

      _collector.ack(tuple)
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

  def main(args: Array[String]): Unit = {
    val file = new java.io.File(args.head)
    inspect("here", file) match{
      case Some(result) => println(result)
      case None => println("Could not read file " + file)
    }
  }
}
