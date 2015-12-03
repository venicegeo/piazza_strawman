import sbt._
import Keys._
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import sbtassembly.AssemblyPlugin
import spray.revolver.RevolverPlugin._
import play.twirl.sbt.SbtTwirl

object GeoIntMessaging extends Build {
  val commonSettings = Seq(
    resolvers ++= Seq(
        "twitter4j" at "http://twitter4j.org/maven2",
        "clojars.org" at "http://clojars.org/repo"
    ),
    organization := "com.radiantblue",
    version := "0.1-SNAPSHOT",
    scalacOptions ++= List("-feature", "-deprecation"),
    scalaVersion := "2.10.6")

  lazy val root = project
    .in(file("."))
    .disablePlugins(AssemblyPlugin)
    .aggregate(core, ogcproxy, uploader, normalizer, deployer, postgres, kafka)
    .settings(commonSettings: _*)
  lazy val core = project
    .disablePlugins(AssemblyPlugin)
    .settings(commonSettings: _*)
  lazy val uploader = project
    .enablePlugins(JavaAppPackaging, SbtTwirl)
    .disablePlugins(AssemblyPlugin)
    .settings(commonSettings: _*)
    .settings(Revolver.settings: _*)
    .dependsOn(core, deployer, kafka, postgres)
  lazy val normalizer = project
    .settings(commonSettings: _*)
    .dependsOn(core, deployer, kafka)
  lazy val ogcproxy = project
    .enablePlugins(JavaAppPackaging)
    .disablePlugins(AssemblyPlugin)
    .settings(commonSettings: _*)
    .settings(Revolver.settings: _*)
    .dependsOn(deployer)
  lazy val kafka = project
    .disablePlugins(AssemblyPlugin)
    .settings(commonSettings: _*)
    .dependsOn(core)
  lazy val postgres = project
    .disablePlugins(AssemblyPlugin)
    .settings(commonSettings: _*)
    .dependsOn(core)
  lazy val deployer = project
    .enablePlugins(JavaAppPackaging)
    .disablePlugins(AssemblyPlugin)
    .settings(commonSettings: _*)
    .dependsOn(core, postgres)
}
