import sbt._
import Keys._
import sbtprotobuf.{ ProtobufPlugin => PB }

object GeoIntMessaging extends Build {
  val commonSettings = Seq(
    scalaVersion := "2.10.6")

  lazy val root = project.in(file(".")).aggregate(messages, uploader, normalizer, persister).settings(commonSettings: _*)
  lazy val messages = project.settings(commonSettings: _*).settings(PB.protobufSettings: _*)
  lazy val uploader = project.settings(commonSettings: _*)
  lazy val normalizer = project.settings(commonSettings: _*)
  lazy val persister = project.settings(commonSettings: _*)
}
