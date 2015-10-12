package com.radiantblue.normalizer

import scala.collection.JavaConverters._
import com.radiantblue.normalizer.mapper._
import com.radiantblue.geoint.Messages

object PersistMetadata {
  private val props = new java.util.Properties()
  props.put("user", "geoint")
  props.put("password", "secret")

  private class PersistBolt extends backtype.storm.topology.base.BaseRichBolt {
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      // TODO: There is a pending 'storm-jdbc' module that looks like it will
      // land in Storm 0.10 and provide connection pooling in a Storm-friendly
      // way.  For now we just connect and disconnect for each tuple processed
      // (slow!)
      val metadata = tuple.getValue(0).asInstanceOf[Messages.Metadata]
      val conn = java.sql.DriverManager.getConnection("jdbc:postgresql://localhost/metadata", props)
      try {
        val pstmt = conn.prepareStatement("INSERT INTO metadata (name, checksum, size) VALUES (?, ?, ?)")
        pstmt.setString(1, metadata.getName)
        pstmt.setString(2, metadata.getChecksum.asScala.map(b => f"$b%02x").mkString)
        pstmt.setLong(3, metadata.getSize)
        pstmt.executeUpdate()
        _collector.ack(tuple)
      } finally {
        conn.close()
      }
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declare(new backtype.storm.tuple.Fields("name", "checksum", "size"))
    }
  }

  def main(args: Array[String]): Unit = {
    java.lang.Class.forName("org.postgresql.Driver")
      
    val kafkaSpout = Kafka.spoutForTopic("metadata", MetadataScheme) 

    val builder = new backtype.storm.topology.TopologyBuilder
    builder.setSpout("metadata", kafkaSpout)
    builder.setBolt("persister", new PersistBolt).shuffleGrouping("metadata")

    val conf = Kafka.topologyConfig

    backtype.storm.StormSubmitter.submitTopology("PersistMetadata", conf, builder.createTopology)
  }
}
