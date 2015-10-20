import sbt._
import Keys._
import sbtprotobuf.{ ProtobufPlugin => PB }
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import sbtassembly.AssemblyPlugin.assemblySettings
import spray.revolver.RevolverPlugin._

object GeoIntMessaging extends Build {
  val commonSettings = Seq(
    resolvers ++= Seq(
        "twitter4j" at "http://twitter4j.org/maven2",
        "clojars.org" at "http://clojars.org/repo"
    ),
    organization := "com.radiantblue",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.10.6")

  lazy val root = project.in(file(".")).aggregate(messages, ogcproxy, uploader, normalizer).settings(commonSettings: _*)
  lazy val messages = project.settings(commonSettings: _*).settings(PB.protobufSettings: _*)
  lazy val uploader = project.settings(commonSettings: _*).settings(Revolver.settings: _*).enablePlugins(JavaAppPackaging).dependsOn(messages)
  lazy val normalizer = project.settings(commonSettings: _*).settings(assemblySettings: _*).dependsOn(messages)
  lazy val ogcproxy = project.settings(commonSettings: _*).settings(Revolver.settings: _*).enablePlugins(JavaAppPackaging)
}
