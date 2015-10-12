package com.radiantblue.normalizer

import com.radiantblue.normalizer.mapper._
import com.radiantblue.geoint.Messages

object ExtractMetadata {
  private class MetadataBolt extends backtype.storm.topology.base.BaseRichBolt {
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val upload = tuple.getValue(0).asInstanceOf[Messages.Upload]
      val path = java.nio.file.Paths.get(new java.net.URI(upload.getLocator))
      val size = java.nio.file.Files.getAttribute(path, "size").asInstanceOf[java.lang.Long]
      val checksum = {
        val channel = java.nio.file.Files.newByteChannel(path)
        try {
          val buff = java.nio.ByteBuffer.allocate(16384)
          var amountRead = 0
          val digest = java.security.MessageDigest.getInstance("MD5")
          while ({ amountRead = channel.read(buff) ; amountRead >= 0 }) {
            digest.update(buff.array(), 0, amountRead)
          }
          digest.digest()
        } finally channel.close()
      }

      _collector.emit(java.util.Arrays.asList(upload.getName, upload.getLocator, checksum, size))
      _collector.ack(tuple)
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declare(new backtype.storm.tuple.Fields("name", "locator", "checksum", "size"))
    }
  }

  def main(args: Array[String]): Unit = {
    val kafkaSpout = Kafka.spoutForTopic("uploads", UploadScheme) 
    val kafkaBolt = Kafka.boltForTopic("metadata", MetadataTupleMapper)

    val builder = new backtype.storm.topology.TopologyBuilder
    builder.setSpout("uploads", kafkaSpout)
    builder.setBolt("metadata", new MetadataBolt).shuffleGrouping("uploads")
    builder.setBolt("publish", kafkaBolt).shuffleGrouping("metadata")

    val conf = Kafka.topologyConfig

    backtype.storm.StormSubmitter.submitTopology("ExtractMetadata", conf, builder.createTopology)
  }
}
