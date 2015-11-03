package com.radiantblue.normalizer

import com.radiantblue.piazza.Messages._

object Inspect {
  val bolt: backtype.storm.topology.IRichBolt  = new backtype.storm.topology.base.BaseRichBolt {
    val logger = org.slf4j.LoggerFactory.getLogger(Inspect.getClass)
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val upload = tuple.getValue(0).asInstanceOf[Upload]
      logger.info("Upload {}", upload)
      import scala.concurrent.ExecutionContext.Implicits.global
      val pathF = (new com.radiantblue.deployer.FileSystemDatasetStorage()).lookup(upload.getLocator)
      val path = scala.concurrent.Await.result(pathF, scala.concurrent.duration.Duration.Inf)
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

      val message = Metadata.newBuilder
        .setName(upload.getName)
        .setLocator(upload.getLocator)
        .setChecksum(com.google.protobuf.ByteString.copyFrom(checksum))
        .setSize(size)
        .build
      _collector.emit(tuple, java.util.Arrays.asList[AnyRef](message.toByteArray))
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
