libraryDependencies ++= {
  val akkaV = "2.3.9"
  val sprayV = "1.3.3"
  Seq(
    "org.apache.kafka" %% "kafka" % "0.8.2.2",
    "org.postgresql" % "postgresql" % "9.4-1203-jdbc42",
    "io.spray"            %%  "spray-can"     % sprayV,
    "io.spray"            %%  "spray-json"     % "1.3.2",
    "io.spray"            %%  "spray-routing" % sprayV,
    "io.spray"            %%  "spray-testkit" % sprayV  % "test",
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"  % akkaV   % "test",
    "org.specs2"          %%  "specs2-core"   % "2.3.7" % "test"
  )
}

mainClass in Compile := Some("com.radiantblue.piazza.web.Boot")
