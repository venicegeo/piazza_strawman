package com.radiantblue.normalizer

import com.radiantblue.piazza.Messages

import scala.collection.JavaConverters._

object InspectGeoTiff {
  private def geoMetadata(
    locator: String,
    crs: org.opengis.referencing.crs.CoordinateReferenceSystem,
    envelope: org.opengis.geometry.BoundingBox
  ): Messages.GeoMetadata = {
    val hasEpsgAuthority: org.opengis.metadata.Identifier => Boolean = 
      _.getAuthority.getIdentifiers.asScala.exists(_.getCode == "EPSG")
    val srid = crs.getIdentifiers.asScala.find(hasEpsgAuthority).map(_.getCode).get
    val latLonEnvelope = {
      import org.geotools.referencing.CRS
      val wgs84 = CRS.decode("EPSG:4326")
      val tx = CRS.findMathTransform(crs, wgs84)
      CRS.transform(tx, envelope)
    }

    (Messages.GeoMetadata.newBuilder
      .setLocator(locator)
      .setCrsCode(srid)
      .setNativeBoundingBox(toBoundingBox(envelope))
      .setLatitudeLongitudeBoundingBox(toBoundingBox(latLonEnvelope))
      .build())
  }

  private def toBoundingBox(e: org.opengis.geometry.Envelope): Messages.GeoMetadata.BoundingBox = 
    (Messages.GeoMetadata.BoundingBox.newBuilder
      .setMinX(e.getMinimum(0))
      .setMaxX(e.getMaximum(0))
      .setMinY(e.getMinimum(1))
      .setMaxY(e.getMaximum(1))
      .build())

  private def tryFindFormat(x: AnyRef): Option[org.geotools.coverage.grid.io.AbstractGridFormat] = 
    try
      Some(org.geotools.coverage.grid.io.GridFormatFinder.findFormat(x))
        .filterNot(_.isInstanceOf[org.geotools.coverage.grid.io.UnknownFormat])
    catch {
      case _: java.lang.UnsupportedOperationException => None
    }

    def inspect(locator: String, file: java.io.File): Option[Messages.GeoMetadata] = {
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
