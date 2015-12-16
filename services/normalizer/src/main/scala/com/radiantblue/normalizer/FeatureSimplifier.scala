package com.radiantblue.normalizer

import kafka.consumer.Whitelist
import scala.collection.JavaConverters._
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier.simplify
import com.radiantblue.piazza.{ kafka => _, _}
import spray.json._
import JsonProtocol._

object FeatureSimplifier {
  val logger = org.slf4j.LoggerFactory.getLogger(FeatureSimplifier.getClass)

  def thread(f: => Any): java.lang.Thread =
    new java.lang.Thread {
      override def run(): Unit = { f }
      start()
    }

  val parseRequest = fromJsonBytes[RequestSimplify]
  val parseGrant = fromJsonBytes[LeaseGranted]

  def main(args: Array[String]): Unit = {
    val producer = com.radiantblue.piazza.kafka.Kafka.producer[String, Array[Byte]]()
    val consumer = com.radiantblue.piazza.kafka.Kafka.consumer("feature-simplifier")
    val requestStreams = consumer.createMessageStreamsByFilter(Whitelist("simplify-requests"))
    val requestThreads = requestStreams.map { stream =>
      thread {
        stream.foreach { message =>
          try {
            val simplify = parseRequest(message.message)
            receiveJob(simplify.locator, simplify.tolerance, producer)
            logger.debug("Simplify {}", simplify)
          } catch {
            case scala.util.control.NonFatal(ex) => logger.error("Failure in simplifier service", ex)
          }
        }
      }
    }
    val simplifyStreams = consumer.createMessageStreamsByFilter(Whitelist("lease-grants"))
    val simplifyThreads = simplifyStreams.map { stream =>
      thread {
        stream.foreach { message =>
          try {
            val grant = parseGrant(message.message)
            receiveLease(grant, producer)
            logger.info("Grant {}", grant)
          } catch {
            case scala.util.control.NonFatal(ex) => logger.error("Failure in simplifier executor", ex)
          }
        }
      }
    }
    (simplifyThreads ++ requestThreads).foreach { _.join() }
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

  // def main(args: Array[String]): Unit = {
  //   val Array(source, tolerance) = args
  //   val sink = java.nio.file.Files.createTempDirectory("feature-simplifier-work").toFile
  //   run(new java.net.URL(source), sink, tolerance.toDouble)
  //   println(zipCompress(sink))
  // }

  private def getResultFile: java.io.File =
    java.nio.file.Files.createTempDirectory("feature-simplifier-work").toFile
  private def requestLease(locator: String, timeout: Long, tag: Array[Byte], producer: kafka.producer.Producer[String, Array[Byte]]): Unit = {
    val message = RequestLease(
      locator=locator,
      timeout=timeout,
      tag=tag.to[Vector])
    val keyedMessage = new kafka.producer.KeyedMessage[String, Array[Byte]]("lease-requests", message.toJson.compactPrint.getBytes("utf-8"))
    producer.send(keyedMessage)
  }

  private def wfsUrl(g: LeaseGranted): java.net.URL =
    new java.net.URL(
      s"http://192.168.23.11:8080/api/deployments?dataset=${g.locator}&SERVICE=WFS&VERSION=1.0.0&REQUEST=GetCapabilities")

  private def publish(file: java.io.File, producer: kafka.producer.Producer[String, Array[Byte]]): Unit = {
    val message = Upload(
      locator=file.getAbsolutePath,
      name="simplify-result")
    val keyedMessage = new kafka.producer.KeyedMessage[String, Array[Byte]]("uploads", message.toJson.compactPrint.getBytes("utf-8"))
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

  def receiveJob(locator: String, tolerance: Double, producer: kafka.producer.Producer[String, Array[Byte]]): Unit = {
    val tag = randomTag()
    synchronized {
      context += (tag.toSeq -> (locator, tolerance))
    }
    requestLease(locator, 60 * 60 * 1000, tag, producer)
  }

  def receiveLease(g: LeaseGranted, producer: kafka.producer.Producer[String, Array[Byte]]): Unit = {
    val ctx = synchronized {
      val result = context.get(g.tag)
      context -= g.tag
      result
    }

    for ((_, tolerance) <- ctx) {
      val wfsCapabilitiesUrl = wfsUrl(g)
      val resultFile = getResultFile
      run(wfsCapabilitiesUrl, resultFile, tolerance)
      val archive = zipCompress(resultFile)
      publish(archive, producer)
    }
  }
}
