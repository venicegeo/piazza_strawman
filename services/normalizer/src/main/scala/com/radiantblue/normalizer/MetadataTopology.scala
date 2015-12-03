package com.radiantblue.normalizer

import mapper._

import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._

object MetadataTopology {
  def topology(): backtype.storm.generated.StormTopology = {
    val uploads = Kafka.spoutForTopic("uploads", JsonScheme.Uploads)
    val lease = Kafka.spoutForTopic("lease-requests", JsonScheme.RequestLeases)
    val simplifyRequests = Kafka.spoutForTopic("simplify-requests", JsonScheme.RequestSimplifies)
    val leaseGrants = Kafka.spoutForTopic("lease-grants", JsonScheme.LeaseGranteds)
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
    builder.setBolt("leasing", Lease.bolt)
      .shuffleGrouping("lease")
    builder.setBolt("deploy", Deploy.bolt)
      .shuffleGrouping("leasing", "deploy-requests")
    builder.setBolt("publish-leases", leaseSink)
      .shuffleGrouping("leasing", "lease-grants")
      .shuffleGrouping("deploy")

    builder.setSpout("simplify-requests", simplifyRequests)
    builder.setBolt("enqueue-simplification", FeatureSimplifier.leaseBolt).shuffleGrouping("simplify-requests")
    builder.setSpout("lease-grants", leaseGrants)
    builder.setBolt("simplification-processing", FeatureSimplifier.simplifyBolt).shuffleGrouping("lease-grants")

    builder.createTopology()
  }

  def main(args: Array[String]): Unit = {
    val topo = topology()
    val conf = Kafka.topologyConfig
    backtype.storm.StormSubmitter.submitTopology("Metadata", conf, topo)
  }
}

