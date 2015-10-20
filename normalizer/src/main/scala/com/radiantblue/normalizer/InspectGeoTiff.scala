package com.radiantblue.normalizer

import com.radiantblue.geoint.Messages

import scala.collection.JavaConverters._

object InspectGeoTiff {
  private def toBoundingBox(e: org.opengis.geometry.Envelope): Messages.GeoMetadata.BoundingBox = 
    (Messages.GeoMetadata.BoundingBox.newBuilder
      .setMinX(e.getMinimum(0))
      .setMaxX(e.getMaximum(0))
      .setMinY(e.getMinimum(1))
      .setMaxY(e.getMinimum(1))
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
            val hasEpsgAuthority: org.opengis.metadata.Identifier => Boolean = 
              _.getAuthority.getIdentifiers.asScala.exists(_.getCode == "EPSG")
            val crs = coverage.getCoordinateReferenceSystem
            val srid = crs.getIdentifiers.asScala.find(hasEpsgAuthority).map(_.getCode).get
            val latLonEnvelope = 
              try {
                import org.geotools.referencing.CRS
                val wgs84 = CRS.decode("EPSG:4326")
                val tx = CRS.findMathTransform(crs, wgs84)
                Some(CRS.transform(tx, envelope))
              } catch {
                case (_: org.opengis.referencing.FactoryException) | (_: org.opengis.referencing.operation.TransformException) =>
                  None
              }

            (Messages.GeoMetadata.newBuilder
              .setLocator(locator)
              .setCrsCode(s"EPSG:$srid")
              .setNativeBoundingBox(toBoundingBox(envelope))
              .setLatitudeLongitudeBoundingBox(latLonEnvelope.map(toBoundingBox).orNull)
              .build())
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
