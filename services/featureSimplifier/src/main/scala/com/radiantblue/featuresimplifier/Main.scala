package com.radiantblue.featuresimplifier

import scala.collection.JavaConverters._
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier.simplify

object Main {
  def connect(args: (String, java.io.Serializable)*): org.geotools.data.DataStore = {
    org.geotools.data.DataStoreFinder.getDataStore(args.toMap.asJava)
  }

  def createShapefile(url: java.net.URL): org.geotools.data.DataStore = {
    val params = Map[String, java.io.Serializable]("url" -> url).asJava
    val factory = new org.geotools.data.shapefile.ShapefileDataStoreFactory
    factory.createNewDataStore(params)
  }

  def iterate[T]
    (coll: org.geotools.data.simple.SimpleFeatureCollection)
    (f: Iterator[org.opengis.feature.simple.SimpleFeature] => T)
    : T
  = {
    val iter = coll.features
    try f(Iterator.continually(iter).takeWhile(_.hasNext).map(_.next))
    finally iter.close
  }

  def main(args: Array[String]): Unit = {
    val Array(source, sink) = args
    val store = connect("WFSDataStoreFactory:GET_CAPABILITIES_URL" -> new java.net.URL(source))
    try {
      val Seq(typename) = store.getTypeNames.toSeq
      val sinkStore = createShapefile(new java.io.File(sink).toURI.toURL)
      try {
        sinkStore.createSchema(store.getSchema(typename))
        val resultName = sinkStore.getTypeNames.head
        val writer = sinkStore.getFeatureWriter(resultName, null)
        try {
          iterate(store.getFeatureSource(typename).getFeatures) { iter =>
            iter.foreach { inF =>
              val outF = writer.next
              outF.setValue(inF.getValue)
              outF.setDefaultGeometry(simplify(outF.getDefaultGeometry.asInstanceOf[com.vividsolutions.jts.geom.Geometry], 0.1))
            }
          }
        } finally writer.close()
      } finally sinkStore.dispose()
    } finally store.dispose()
  }
}
