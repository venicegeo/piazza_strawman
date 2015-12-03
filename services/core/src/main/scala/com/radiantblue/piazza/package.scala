package com.radiantblue.piazza

sealed trait DeployStatus
final case class Starting(id: Long) extends DeployStatus
final case class Live(id: Long, server: Server) extends DeployStatus
object Killing extends DeployStatus
object Dead extends DeployStatus

final case class Lease(id: Long, deployment: Long, lifetime: Option[java.sql.Timestamp], tag: Array[Byte])

final case class Upload(name: String, locator: String)

final case class Metadata(name: String, locator: String, checksum: Vector[Byte], size: Long)

final case class GeoMetadata(
  locator: String,
  crsCode: String,
  nativeBoundingBox: Bounds,
  latitudeLongitudeBoundingBox: Bounds,
  nativeFormat: String)

final case class Bounds(minX: Double, maxX: Double, minY: Double, maxY: Double)

final case class Server(host: String, port: Int, localPath: String)

final case class RequestLease(locator: String, timeout: Long, tag: Vector[Byte])

final case class LeaseGranted(locator: String, timeout: Long, tag: Vector[Byte])

final case class RequestDeploy(locator: String, deployId: Long, tag: Vector[Byte], server: Server)

final case class RequestSimplify(locator: String, tolerance: Double)

sealed trait JobRequest
final case class Submit(id: String, service: String, parameters: Vector[String]) extends JobRequest
final case class Report(status: JobStatus) extends JobRequest

sealed trait JobStatus {
  def id: String
}

final case class Pending(id: String, serviceName: String, params: Vector[String]) extends JobStatus
final case class Assigned(id: String, node: String) extends JobStatus
final case class Started(id: String, node: String) extends JobStatus
final case class Progress(id: String, progress: Double) extends JobStatus
final case class Done(id: String, result: String) extends JobStatus
final case class Failed(id: String, reason: String) extends JobStatus
final case class Aborted(id: String) extends JobStatus
