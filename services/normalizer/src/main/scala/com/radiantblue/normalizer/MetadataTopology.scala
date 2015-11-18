package com.radiantblue.normalizer

import mapper._

object MetadataTopology {
  def main(args: Array[String]): Unit = {
    val uploads = Kafka.spoutForTopic("uploads", UploadScheme)
    val lease = Kafka.spoutForTopic("lease-requests", LeaseScheme)
    val simplifyRequests = Kafka.spoutForTopic("simplify-requests", SimplifyScheme)
    val leaseGrants = Kafka.spoutForTopic("lease-grants", LeaseGrantScheme)
    val metadataSink = Kafka.boltForTopic("metadata", DirectTupleMapper)
    val leaseSink = Kafka.boltForTopic("lease-grants", DirectTupleMapper)

    val builder = new backtype.storm.topology.TopologyBuilder
    builder.setSpout("uploads", uploads)
    builder.setBolt("metadata", Inspect.bolt).shuffleGrouping("uploads")
    builder.setBolt("geotiff-metadata", InspectGeoTiff.bolt).shuffleGrouping("uploads")
    builder.setBolt("zipped-shapefile-metadata", InspectZippedShapefile.bolt).shuffleGrouping("uploads")
    builder.setBolt("publish", metadataSink)
      .shuffleGrouping("metadata")
      .shuffleGrouping("geotiff-metadata")
      .shuffleGrouping("zipped-shapefile-metadata")

    builder.setSpout("lease", lease)
    builder.setBolt("leasing", Lease.bolt).shuffleGrouping("lease")
    builder.setBolt("publish-leases", leaseSink).shuffleGrouping("leasing")

    builder.setSpout("simplify-requests", simplifyRequests)
    builder.setBolt("enqueue-simplification", FeatureSimplifier.leaseBolt).shuffleGrouping("simplify-requests")
    builder.setSpout("lease-grants", leaseGrants)
    builder.setBolt("simplification-processing", FeatureSimplifier.simplifyBolt).shuffleGrouping("lease-grants")

    val conf = Kafka.topologyConfig
    backtype.storm.StormSubmitter.submitTopology("Metadata", conf, builder.createTopology)
  }
}
