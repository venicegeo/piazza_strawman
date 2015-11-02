package com.radiantblue.normalizer

import com.radiantblue.piazza.Messages

import scala.collection.JavaConverters._

object InspectZippedShapefile {
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

  def inspect(locator: String, file: java.io.File): Option[Messages.GeoMetadata] = {
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
        Some(geoMetadata("locator", source.getSchema.getCoordinateReferenceSystem, source.getBounds))
      } finally store.dispose()
    } catch {
      case scala.util.control.NonFatal(_) => None
    }
  }
}
