package com.radiantblue.normalizer

import scala.collection.JavaConverters._
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier.simplify
import com.radiantblue.piazza.Messages._

object FeatureSimplifier {
  val leaseBolt: backtype.storm.topology.IRichBolt = new backtype.storm.topology.base.BaseRichBolt {
    val logger = org.slf4j.LoggerFactory.getLogger(FeatureSimplifier.getClass)
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val simplify = tuple.getValue(0).asInstanceOf[Simplify]
      receiveJob(simplify.getLocator, simplify.getTolerance)
      logger.info("Simplify {}", simplify)
      _collector.ack(tuple)
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declare(new backtype.storm.tuple.Fields("message"))
    }
  }

  val simplifyBolt: backtype.storm.topology.IRichBolt = new backtype.storm.topology.base.BaseRichBolt {
    val logger = org.slf4j.LoggerFactory.getLogger(FeatureSimplifier.getClass)
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val grant = tuple.getValue(0).asInstanceOf[LeaseGranted]
      receiveLease(grant)
      logger.info("Simplify {}", grant)
      _collector.ack(tuple)
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declare(new backtype.storm.tuple.Fields("message"))
    }
  }

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

  def run(
    wfsCapabilitiesUrl: java.net.URL,
    resultFile: java.io.File, 
    tolerance: Double)
  : Unit 
  = {
    val store = connect("WFSDataStoreFactory:GET_CAPABILITIES_URL" -> wfsCapabilitiesUrl)
    try {
      val Seq(typename) = store.getTypeNames.toSeq
      val sinkStore = createShapefile(resultFile.toURI.toURL)
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
        } finally writer.close
      } finally sinkStore.dispose
    } finally store.dispose
  }

  def main(args: Array[String]): Unit = {
    val Array(source, tolerance) = args
    val sink = java.nio.file.Files.createTempDirectory("feature-simplifier-work").toFile
    run(new java.net.URL(source), sink, tolerance.toDouble)
    println(zipCompress(sink))
  }

  val producer = com.radiantblue.piazza.kafka.Kafka.producer[String, Array[Byte]]()

  private def getResultFile: java.io.File =
    java.nio.file.Files.createTempDirectory("feature-simplifier-work").toFile
  private def requestLease(locator: String, timeout: Long, tag: Array[Byte]): Unit = {
    val message = RequestLease.newBuilder()
      .setLocator(locator)
      .setTimeout(timeout)
      .setTag(com.google.protobuf.ByteString.copyFrom(tag))
      .build()
    val keyedMessage = new kafka.producer.KeyedMessage[String, Array[Byte]]("lease-requests", message.toByteArray)
    producer.send(keyedMessage)
  }

  private def wfsUrl(g: com.radiantblue.piazza.Messages.LeaseGranted): java.net.URL = 
    new java.net.URL(
      s"http://192.168.23.11:8080/api/deployments?dataset=${g.getLocator}&SERVICE=WFS&VERSION=1.0.0&REQUEST=GetCapabilities")

  private def publish(file: java.io.File): Unit = {
    val message = Upload.newBuilder()
      .setLocator(file.getAbsolutePath)
      .setName("simplify-result")
      .build()
    val keyedMessage = new kafka.producer.KeyedMessage[String, Array[Byte]]("uploads", message.toByteArray)
    producer.send(keyedMessage)
  }

  def walk(f: java.io.File): Iterator[java.io.File] = 
    Iterator(f) ++ Option(f.listFiles: Seq[java.io.File]).getOrElse(Nil).flatMap(walk)

  private def zipCompress(file: java.io.File): java.io.File = {
    val zipFile = java.nio.file.Files.createTempFile("piazza", "simplifier-shapefile-compression")
    val outStream = new java.io.FileOutputStream(zipFile.toFile)
    try {
      val zip = new java.util.zip.ZipOutputStream(outStream)
      try {
        for (f <- walk(file).filterNot(_.isDirectory)) {
          val entry = new java.util.zip.ZipEntry(file.toPath.relativize(f.toPath).toString)
          zip.putNextEntry(entry)
          java.nio.file.Files.copy(f.toPath, zip)
        }
        zipFile.toFile
      } finally zip.close
    } finally outStream.close
  }

  private var context: Map[Seq[Byte], (String, Double)] = Map.empty

  private def randomTag(): Array[Byte] = {
    val buff = java.nio.ByteBuffer.allocate(8)
    buff.putLong(scala.util.Random.nextLong)
    buff.array
  }

  def receiveJob(locator: String, tolerance: Double): Unit = {
    val tag = randomTag()
    synchronized {
      context += (tag.toSeq -> (locator, tolerance))
    }
    requestLease(locator, 60 * 60 * 1000, tag)
  }

  def receiveLease(g: com.radiantblue.piazza.Messages.LeaseGranted): Unit = {
    val key = g.getTag.toByteArray
    val ctx = synchronized {
      val result = context.get(key.toSeq)
      context -= key.toSeq
      result
    }

    for ((_, tolerance) <- ctx) {
      val wfsCapabilitiesUrl = wfsUrl(g)
      val resultFile = getResultFile
      run(wfsCapabilitiesUrl, resultFile, tolerance)
      val archive = zipCompress(resultFile)
      publish(archive)
    }
  }
}
