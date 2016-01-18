resolvers ++= Seq(
  "OSGeo" at "http://download.osgeo.org/webdav/geotools/",
  "geosolutions" at "http://maven.geo-solutions.it/")

libraryDependencies ++= {
  val geotoolsV = "14.0"
  Seq(
    "org.apache.kafka" %% "kafka" % "0.9.0.0",
    "org.slf4j" % "slf4j-api" % "1.6.6",
    "org.apache.kafka" % "kafka-clients" % "0.9.0.0",
    "org.postgresql" % "postgresql" % "9.4-1203-jdbc42",
    "org.geotools" % "gt-shapefile" % geotoolsV,
    "org.geotools" % "gt-wfs" % geotoolsV,
    "org.geotools" % "gt-geotiff" % geotoolsV,
    "org.geotools" % "gt-referencing" % geotoolsV,
    "org.geotools" % "gt-epsg-hsql" % geotoolsV
  )
}

mainClass in Compile := Some("com.radiantblue.normalizer.Persist")
