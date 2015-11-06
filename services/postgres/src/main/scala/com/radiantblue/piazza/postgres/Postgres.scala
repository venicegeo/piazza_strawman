package com.radiantblue.piazza.postgres

import scala.collection.JavaConverters._

case class Postgres(config: com.typesafe.config.Config) {
  lazy val uri: String = config.getString("uri")
  lazy val properties: java.util.Properties = {
    val props = new java.util.Properties()
    for {
      entry <- config.getConfig("properties").entrySet.asScala
      if entry.getValue.valueType == com.typesafe.config.ConfigValueType.STRING
    } props.put(entry.getKey, entry.getValue.unwrapped)
    props
  }

  private lazy val parsedUri = new java.net.URI(new java.net.URI(uri).getSchemeSpecificPart)

  def user = properties.get("properties.user").asInstanceOf[String]
  def password = properties.get("properties.password").asInstanceOf[String]
  def host = parsedUri.getHost
  def port = if (parsedUri.getPort < 0) 5432 else parsedUri.getPort
  def database = parsedUri.getPath.drop("/".length)

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

