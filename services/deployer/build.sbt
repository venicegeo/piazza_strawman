libraryDependencies ++= {
  val akkaV = "2.3.9"
  val sprayV = "1.3.3"
  Seq(
    "org.postgresql" % "postgresql" % "9.4-1203-jdbc42",
    "io.spray"            %%  "spray-client"     % sprayV,
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"  % akkaV   % "test"
  )
}

mainClass in Compile := Some("com.radiantblue.deployer.Deployer")
