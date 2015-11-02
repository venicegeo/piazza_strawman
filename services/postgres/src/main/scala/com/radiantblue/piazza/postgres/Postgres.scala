package com.radiantblue.piazza.postgres

import scala.collection.JavaConverters._

object Postgres {
  lazy val config = com.typesafe.config.ConfigFactory.load()

  def uri: String = config.getString("piazza.postgres.uri")
  def properties: java.util.Properties = {
    val props = new java.util.Properties()
    for {
      entry <- config.getConfig("piazza.postgres.properties").entrySet.asScala
      if entry.getValue.valueType == com.typesafe.config.ConfigValueType.STRING
    } props.put(entry.getKey, entry.getValue.unwrapped)
    props
  }

  def connect(): java.sql.Connection = {
    Class.forName("org.postgresql.Driver")
    java.sql.DriverManager.getConnection(uri, properties)
  }
}
