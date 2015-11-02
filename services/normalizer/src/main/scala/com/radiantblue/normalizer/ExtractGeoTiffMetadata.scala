package com.radiantblue.normalizer

import com.radiantblue.normalizer.mapper._
import com.radiantblue.piazza.Messages

object ExtractGeoTiffMetadata {
  private class GeoMetadataBolt extends backtype.storm.topology.base.BaseRichBolt {
    val logger = org.slf4j.LoggerFactory.getLogger(classOf[GeoMetadataBolt])
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val upload = tuple.getValue(0).asInstanceOf[Messages.Upload]
      logger.info("Upload {}", upload)
      import scala.concurrent.ExecutionContext.Implicits.global
      val pathF = (new com.radiantblue.deployer.FileSystemDatasetStorage()).lookup(upload.getLocator)
      val path = scala.concurrent.Await.result(pathF, scala.concurrent.duration.Duration.Inf)
      logger.info("path {}", path)
      val result = InspectGeoTiff.inspect(upload.getLocator, path.toFile)

      result.foreach { r =>
        _collector.emit(tuple, java.util.Arrays.asList[AnyRef](r.toByteArray))
        logger.info("emitted")
      }

      _collector.ack(tuple)
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declare(new backtype.storm.tuple.Fields("message"))
    }
  }

  def main(args: Array[String]): Unit = {
    val kafkaSpout = Kafka.spoutForTopic("uploads", UploadScheme) 
    val kafkaBolt = Kafka.boltForTopic("metadata", DirectTupleMapper)

    val builder = new backtype.storm.topology.TopologyBuilder
    builder.setSpout("uploads", kafkaSpout)
    builder.setBolt("metadata", new GeoMetadataBolt).shuffleGrouping("uploads")
    builder.setBolt("publish", kafkaBolt).shuffleGrouping("metadata")

    val conf = Kafka.topologyConfig

    backtype.storm.StormSubmitter.submitTopology("ExtractGeoTiffMetadata", conf, builder.createTopology)
  }
}
