libraryDependencies ++= Seq(
  "org.apache.storm" % "storm-core" % "0.9.5" % "provided",
  "org.apache.storm" % "storm-kafka" % "0.9.5",
  "org.apache.kafka" %% "kafka" % "0.8.2.2" excludeAll(
    ExclusionRule(organization = "org.apache.zookeeper", artifact="zookeeper"),
    ExclusionRule(organization = "log4j", artifact="log4j")),
  "org.apache.kafka" % "kafka-clients" % "0.8.2.2")
