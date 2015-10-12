package com.radiantblue.normalizer.mapper

import com.radiantblue.geoint.Messages

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

object MetadataScheme extends backtype.storm.spout.Scheme {
  def deserialize(bytes: Array[Byte]): java.util.List[AnyRef] =
    java.util.Arrays.asList(com.radiantblue.geoint.Messages.Metadata.parseFrom(bytes))
  def getOutputFields(): backtype.storm.tuple.Fields =
    new backtype.storm.tuple.Fields("metadata")
}

object UploadScheme extends backtype.storm.spout.Scheme {
  def deserialize(bytes: Array[Byte]): java.util.List[AnyRef] =
    java.util.Arrays.asList(com.radiantblue.geoint.Messages.Upload.parseFrom(bytes))
  def getOutputFields(): backtype.storm.tuple.Fields =
    new backtype.storm.tuple.Fields("upload")
}
