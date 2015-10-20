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
      val conn = java.sql.DriverManager.getConnection("jdbc:postgresql://192.168.23.12/metadata", props)
      try {
        tuple.getValue(0) match {
          case metadata: Messages.Metadata =>
            val pstmt = conn.prepareStatement("INSERT INTO metadata (name, locator, checksum, size) VALUES (?, ?, ?, ?)")
            pstmt.setString(1, metadata.getName)
            pstmt.setString(2, metadata.getLocator)
            pstmt.setString(3, metadata.getChecksum.asScala.map(b => f"$b%02x").mkString)
            pstmt.setLong(4, metadata.getSize)
            pstmt.executeUpdate()
          case geoMetadata: Messages.GeoMetadata => 
            val pstmt = conn.prepareStatement("""
INSERT INTO geometadata (locator, native_srid, native_bounds, latlon_bounds) VALUES ( 
  ?,
  ?, 
  ST_MakeBox2D(ST_Point(?, ?), ST_Point(?, ?)),
  ST_MakeBox2D(ST_Point(?, ?), ST_Point(?, ?))
)
""")
            pstmt.setString(1, geoMetadata.getLocator)
            pstmt.setString(2, geoMetadata.getCrsCode)
            pstmt.setDouble(3, geoMetadata.getNativeBoundingBox.getMinX)
            pstmt.setDouble(4, geoMetadata.getNativeBoundingBox.getMinY)
            pstmt.setDouble(5, geoMetadata.getNativeBoundingBox.getMaxX)
            pstmt.setDouble(6, geoMetadata.getNativeBoundingBox.getMaxY)
            pstmt.setDouble(7, geoMetadata.getLatitudeLongitudeBoundingBox.getMinX)
            pstmt.setDouble(8, geoMetadata.getLatitudeLongitudeBoundingBox.getMinY)
            pstmt.setDouble(9, geoMetadata.getLatitudeLongitudeBoundingBox.getMaxX)
            pstmt.setDouble(10, geoMetadata.getLatitudeLongitudeBoundingBox.getMaxY)
            pstmt.executeUpdate()
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
