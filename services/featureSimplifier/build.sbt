resolvers ++= Seq(
  "OSGeo" at "http://download.osgeo.org/webdav/geotools/",
  "geosolutions" at "http://maven.geo-solutions.it/")

libraryDependencies ++= {
  val geotoolsV = "14.0"
  Seq(
    "org.geotools" % "gt-shapefile" % geotoolsV,
    "org.geotools" % "gt-wfs" % geotoolsV,
    "org.geotools" % "gt-referencing" % geotoolsV,
    "org.geotools" % "gt-epsg-hsql" % geotoolsV)
}
