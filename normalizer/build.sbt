libraryDependencies ++= Seq(
  "org.apache.storm" % "storm-core" % "0.9.5" % "provided",
  "org.apache.storm" % "storm-kafka" % "0.9.5",
  "org.apache.kafka" %% "kafka" % "0.8.2.2" excludeAll(
    ExclusionRule(organization = "org.apache.zookeeper", artifact="zookeeper"),
    ExclusionRule(organization = "log4j", artifact="log4j")),
  "org.slf4j" % "slf4j-api" % "1.6.6",
  "org.slf4j" % "slf4j-simple" % "1.6.6",
  "org.apache.kafka" % "kafka-clients" % "0.8.2.2",
  "org.postgresql" % "postgresql" % "9.4-1203-jdbc42")
