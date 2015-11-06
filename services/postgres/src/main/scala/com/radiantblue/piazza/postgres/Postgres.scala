package com.radiantblue.piazza.postgres

import scala.collection.JavaConverters._

case class Postgres(config: com.typesafe.config.Config) {
  def uri: String = config.getString("uri")
  def properties: java.util.Properties = {
    val props = new java.util.Properties()
    for {
      entry <- config.getConfig("properties").entrySet.asScala
      if entry.getValue.valueType == com.typesafe.config.ConfigValueType.STRING
    } props.put(entry.getKey, entry.getValue.unwrapped)
    props
  }

  def connect(): java.sql.Connection = {
    Class.forName("org.postgresql.Driver")
    java.sql.DriverManager.getConnection(uri, properties)
  }
}

object Postgres {
  private lazy val config = com.typesafe.config.ConfigFactory.load()

  def apply(section: String): Postgres = 
    new Postgres(config.getConfig(section))
}

