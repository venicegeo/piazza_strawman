package com.radiantblue.normalizer

import scala.collection.JavaConverters._
import com.radiantblue.normalizer.mapper._
import com.radiantblue.piazza.Messages
import com.radiantblue.piazza.postgres._

object PersistMetadata {
  private class PersistBolt extends backtype.storm.topology.base.BaseRichBolt {
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      // TODO: There is a pending 'storm-jdbc' module that looks like it will
      // land in Storm 0.10 and provide connection pooling in a Storm-friendly
      // way.  For now, just connect and disconnect for each tuple processed
      // (slow!)
      val conn = Postgres.connect()
      try {
        tuple.getValue(0) match {
          case metadata: Messages.Metadata => conn.insertMetadata(metadata)
          case geoMetadata: Messages.GeoMetadata => conn.insertGeoMetadata(geoMetadata)
        }
        _collector.ack(tuple)
      } finally {
        conn.close()
      }
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      // no output
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
