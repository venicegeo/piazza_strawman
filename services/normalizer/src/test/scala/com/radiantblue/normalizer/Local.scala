package com.radiantblue.normalizer

object Local {
  def main(args: Array[String]) {
    val cluster = new backtype.storm.LocalCluster
    cluster.submitTopology("Metadata", Kafka.topologyConfig, MetadataTopology.topology)
  }
}

