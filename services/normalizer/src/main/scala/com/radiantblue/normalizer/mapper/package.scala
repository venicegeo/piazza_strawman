package com.radiantblue.normalizer.mapper
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._
import spray.json._

object MetadataTupleMapper extends storm.kafka.bolt.mapper.TupleToKafkaMapper[String, Array[Byte]] {
  def getKeyFromTuple(tuple: backtype.storm.tuple.Tuple): String = null
  def getMessageFromTuple(tuple: backtype.storm.tuple.Tuple): Array[Byte] =
    try {
      val name = tuple.getValueByField("name").asInstanceOf[String]
      val locator = tuple.getValueByField("locator").asInstanceOf[String]
      val checksum = tuple.getValueByField("checksum").asInstanceOf[Vector[Byte]]
      val size = tuple.getValueByField("size").asInstanceOf[Long]
      Metadata(
        name=name,
        locator=locator,
        checksum=checksum,
        size=size).toJson.compactPrint.getBytes("utf-8")
    } catch {
      case e: java.io.UnsupportedEncodingException => throw new RuntimeException(e)
    }
}

object DirectTupleMapper extends storm.kafka.bolt.mapper.TupleToKafkaMapper[String, Array[Byte]] {
  def getKeyFromTuple(tuple: backtype.storm.tuple.Tuple): String = null
  def getMessageFromTuple(tuple: backtype.storm.tuple.Tuple): Array[Byte] =
    tuple.getValue(0).asInstanceOf[Array[Byte]]
}

trait JsonScheme extends backtype.storm.spout.Scheme {
  implicit def outFormat: Array[Byte] => AnyRef

  def deserialize(bytes: Array[Byte]): java.util.List[AnyRef] = {
    val out = outFormat(bytes)
    java.util.Arrays.asList(out)
  }

  def getOutputFields(): backtype.storm.tuple.Fields =
    new backtype.storm.tuple.Fields("message")
}

object JsonScheme {
  object Uploads extends JsonScheme {
    def outFormat = fromJsonBytes[Upload]
  }

  object RequestLeases extends JsonScheme {
    def outFormat = fromJsonBytes[RequestLease]
  }

  object RequestSimplifies extends JsonScheme {
    def outFormat = fromJsonBytes[RequestSimplify]
  }

  object LeaseGranteds extends JsonScheme {
    def outFormat = fromJsonBytes[LeaseGranted]
  }

  object Metadatas extends JsonScheme {
    def outFormat = fromJsonBytes[Either[Metadata, GeoMetadata]]
  }
}
