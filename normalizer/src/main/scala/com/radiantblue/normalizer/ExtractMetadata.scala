package com.radiantblue.normalizer

import com.radiantblue.geoint.Messages

object ExtractMetadata {
  private object MetadataTupleMapper extends storm.kafka.bolt.mapper.TupleToKafkaMapper[String, Array[Byte]] {
    def getKeyFromTuple(tuple: backtype.storm.tuple.Tuple): String = null
    def getMessageFromTuple(tuple: backtype.storm.tuple.Tuple): Array[Byte] = 
      try {
        val name = tuple.getValueByField("name").asInstanceOf[String]
        val checksum = tuple.getValueByField("checksum").asInstanceOf[Array[Byte]]
        val size = tuple.getValueByField("size").asInstanceOf[Long]
        (Messages.Metadata.newBuilder()
          .setName(name)
          .setChecksum(com.google.protobuf.ByteString.copyFrom(checksum))
          .setSize(size)).build.toByteArray
      } catch {
        case e: java.io.UnsupportedEncodingException => throw new RuntimeException(e)
      }
  }

  private object UploadScheme extends backtype.storm.spout.Scheme {
    def deserialize(bytes: Array[Byte]): java.util.List[AnyRef] =
      java.util.Arrays.asList(com.radiantblue.geoint.Messages.Upload.parseFrom(bytes))
    def getOutputFields(): backtype.storm.tuple.Fields =
      new backtype.storm.tuple.Fields("upload")
  }

  private class MetadataBolt extends backtype.storm.topology.base.BaseRichBolt {
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val upload = tuple.getValue(0).asInstanceOf[Messages.Upload]
      _collector.emit(java.util.Arrays.asList(upload.getName(), Array[Byte](0x1A, 0x2B, 0x3C), 512: java.lang.Long))
      _collector.ack(tuple)
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declare(new backtype.storm.tuple.Fields("name", "checksum", "size"))
    }
  }

  def main(args: Array[String]): Unit = {
    val hosts = new storm.kafka.ZkHosts("localhost:2181")
    val spoutConfig = new storm.kafka.SpoutConfig(hosts, "uploads", "", "uploads")
    spoutConfig.scheme = new backtype.storm.spout.SchemeAsMultiScheme(UploadScheme)
    val kafkaSpout = new storm.kafka.KafkaSpout(spoutConfig)

    val kafkaBolt = (new storm.kafka.bolt.KafkaBolt()
      .withTopicSelector(new storm.kafka.bolt.selector.DefaultTopicSelector("metadata"))
      .withTupleToKafkaMapper(MetadataTupleMapper))
    val builder = new backtype.storm.topology.TopologyBuilder
    builder.setSpout("uploads", kafkaSpout)
    builder.setBolt("metadata", new MetadataBolt).shuffleGrouping("uploads")
    builder.setBolt("publish", kafkaBolt).shuffleGrouping("metadata")

    val conf = new backtype.storm.Config
    val props = new java.util.Properties
    props.put("metadata.broker.list", "localhost:9092")
    conf.put(storm.kafka.bolt.KafkaBolt.KAFKA_BROKER_PROPERTIES, props)

    backtype.storm.StormSubmitter.submitTopology("ExtractMetadata", conf, builder.createTopology)
  }
}
