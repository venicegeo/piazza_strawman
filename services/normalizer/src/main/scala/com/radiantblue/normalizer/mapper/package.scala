package com.radiantblue.normalizer.mapper

import com.radiantblue.piazza.Messages

object MetadataTupleMapper extends storm.kafka.bolt.mapper.TupleToKafkaMapper[String, Array[Byte]] {
  def getKeyFromTuple(tuple: backtype.storm.tuple.Tuple): String = null
  def getMessageFromTuple(tuple: backtype.storm.tuple.Tuple): Array[Byte] = 
    try {
      val name = tuple.getValueByField("name").asInstanceOf[String]
      val locator = tuple.getValueByField("locator").asInstanceOf[String]
      val checksum = tuple.getValueByField("checksum").asInstanceOf[Array[Byte]]
      val size = tuple.getValueByField("size").asInstanceOf[Long]
      (Messages.Metadata.newBuilder()
        .setName(name)
        .setLocator(locator)
        .setChecksum(com.google.protobuf.ByteString.copyFrom(checksum))
        .setSize(size)).build.toByteArray
    } catch {
      case e: java.io.UnsupportedEncodingException => throw new RuntimeException(e)
    }
}

object DirectTupleMapper extends storm.kafka.bolt.mapper.TupleToKafkaMapper[String, Array[Byte]] {
  def getKeyFromTuple(tuple: backtype.storm.tuple.Tuple): String = null
  def getMessageFromTuple(tuple: backtype.storm.tuple.Tuple): Array[Byte] = 
    tuple.getValue(0).asInstanceOf[Array[Byte]]
}

object MetadataScheme extends backtype.storm.spout.Scheme {
  private def maybe[T](parse: => T): Option[T] = 
    try 
      Some(parse)
    catch {
      case _: com.google.protobuf.InvalidProtocolBufferException => None
    }

  def deserialize(bytes: Array[Byte]): java.util.List[AnyRef] = {
    val result = (
      maybe(Messages.Metadata.parseFrom(bytes)) orElse 
      maybe(Messages.GeoMetadata.parseFrom(bytes))
    )
    java.util.Arrays.asList(result.get)
  }
  def getOutputFields(): backtype.storm.tuple.Fields =
    new backtype.storm.tuple.Fields("metadata")
}

object UploadScheme extends backtype.storm.spout.Scheme {
  def deserialize(bytes: Array[Byte]): java.util.List[AnyRef] =
    java.util.Arrays.asList(Messages.Upload.parseFrom(bytes))
  def getOutputFields(): backtype.storm.tuple.Fields =
    new backtype.storm.tuple.Fields("upload")
}

object LeaseScheme extends backtype.storm.spout.Scheme {
  def deserialize(bytes: Array[Byte]): java.util.List[AnyRef] =
    java.util.Arrays.asList(Messages.RequestLease.parseFrom(bytes))
  def getOutputFields(): backtype.storm.tuple.Fields =
    new backtype.storm.tuple.Fields("lease")
}
