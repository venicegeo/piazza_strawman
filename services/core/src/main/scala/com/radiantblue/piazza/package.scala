package com.radiantblue.piazza

// sealed case class Server(address: String, port: String, localFilePath: String)

sealed trait DeployStatus
case class Starting(id: Long) extends DeployStatus
case class Live(id: Long, server: Messages.Server) extends DeployStatus
object Killing extends DeployStatus
object Dead extends DeployStatus

sealed case class Lease(id: Long, deployment: Long, lifetime: Option[java.sql.Timestamp], tag: Array[Byte])
