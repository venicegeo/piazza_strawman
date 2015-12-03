package com.radiantblue.normalizer

import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._

object Inspect {
  val bolt: backtype.storm.topology.IRichBolt  = new backtype.storm.topology.base.BaseRichBolt {
    val logger = org.slf4j.LoggerFactory.getLogger(Inspect.getClass)
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val format = toJsonBytes[Metadata]
      val upload = tuple.getValue(0).asInstanceOf[Upload]
      logger.info("Upload {}", upload)
      val path = (new com.radiantblue.deployer.FileSystemDatasetStorage()).lookup(upload.locator)
      logger.info("path {}", path)
      val size = java.nio.file.Files.getAttribute(path, "size").asInstanceOf[java.lang.Long]
      logger.info("size {}", size)
      val checksum = {
        val stream = new java.io.FileInputStream(path.toFile)
        try {
          val buff = Array.ofDim[Byte](16384)
          var amountRead = 0
          val digest = java.security.MessageDigest.getInstance("MD5")
          while ({
            amountRead = stream.read(buff)
            amountRead >= 0
          }) {
            digest.update(buff, 0, amountRead)
          }
          digest.digest()
        } finally {
          stream.close()
        }
      }
      logger.info("checksum {}", checksum.map(b => f"$b%02X").mkString)

      val message = Metadata(
        name=upload.name,
        locator=upload.locator,
        checksum=checksum.to[Vector],
        size=size)
      _collector.emit(tuple, java.util.Arrays.asList[AnyRef](format(message)))
      logger.info("emitted")
      _collector.ack(tuple)
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declare(new backtype.storm.tuple.Fields("message"))
    }
  }
}
