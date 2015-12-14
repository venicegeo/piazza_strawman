package com.radiantblue.piazza

/**
 * The DeployStatus trait is a message that is sent in response to
 * RequestDeploy, part of the Accessor service.
 */
sealed trait DeployStatus

/**
 * The Starting status indicates that a resource is pending deployment.
 * @param id an identifier for the deployment
 */
final case class Starting(id: Long) extends DeployStatus

/**
 * The Live status indicates a resource is currently deployed and accessible.
 * @param id an identifier for the deployment
 * @param server the server where the resource is accessible
 */
final case class Live(id: Long, server: Server) extends DeployStatus

/**
 * The Dead status indicates that a deployment is no longer accessible.
 */
object Dead extends DeployStatus

/**
 * A Lease indicates a specific client's interest in a deployment.
 * One deployment may serve several leases, and as long as one unexpired lease
 * exists for a deployment, the deployment will not be scheduled for
 * destruction.
 *
 * @param id an identifier for the Lease
 * @param deployment an identifier for the deployment serving the lease
 * @param lifetime a Timestamp indicating the earliest time that the system might revoke the deployment
 * @param tag an arbitrary string of bytes associated with the lease.  The client can include this in the RequestLease and use it to identify a lease since the id is not known before the request is sent.
 */
final case class Lease(id: Long, deployment: Long, lifetime: Option[java.sql.Timestamp], tag: Array[Byte])

/**
 * An Upload is a message indicating a new dataset in the system. This is sent
 * upon receipt of the raw data, and any parsers/normalizers should listen for
 * it to do their work.
 *
 * @param name the filename provided by the uploader
 * @param locator the resource locator assigned by the system
 */
final case class Upload(name: String, locator: String)

/**
 * A Metadata message indicates that metadata from a new dataset has been extracted.
 * @param name the user provided name (filename) for the dataset
 * @param locator the internal identifier for the dataset
 * @param checksum the md5 checksum of the data
 * @param size the size of the data in bytes
 */
final case class Metadata(name: String, locator: String, checksum: Vector[Byte], size: Long)

/**
 * A GeoMetadata message indicates geospatial metadata from a new dataset.
 * @param locator the internal identifier for the dataset
 * @param crsCode the CRS code for the dataset if any exists.
 * @param nativeBoundingBox the bounding box of the data in the same coordinate system that the data is stored in
 * @param latitudeLongitudeBoundingBox the bounding box of the data in latitude/longitude coordinates
 * @param nativeFormat the file format used to store the data
 */
final case class GeoMetadata(
  locator: String,
  crsCode: String,
  nativeBoundingBox: Bounds,
  latitudeLongitudeBoundingBox: Bounds,
  nativeFormat: String)

/**
 * A simple bounding box class so that we can do simple geo stuff without pulling in all of geotools.
 */
final case class Bounds(minX: Double, maxX: Double, minY: Double, maxY: Double)

/**
 * Details of a GeoServer instance
 *
 * @param host the hostname for the server
 * @param port the port on which the server listens
 * @param localPath the file path to the geoserver data directory (where geotiffs and other files directly read by geoserver belong)
 */
final case class Server(host: String, port: Int, localPath: String)

/**
 * The RequestLease message indicates a client's desire to access a particular dataset
 *
 * @param locator the internal identifier for the dataset
 * @param timeout the amount of time that the client needs access for
 * @param tag an arbitrary blob of data that the client will use to recognize the response to this request
 */
final case class RequestLease(locator: String, timeout: Long, tag: Vector[Byte])

/**
 * The LeaseGranted message indicates a deployment satisfying a lease is
 * available. This may be a newly deployed service or an existing deployment
 * may be associated with a new lease.
 *
 * @param locator the internal identifier for the leased dataset
 * @param timeout the time at which the lease is up as a unix timestamp
 * @param tag an arbitrary blob of data that the client will use to associate the response with the request
 */
final case class LeaseGranted(locator: String, timeout: Long, tag: Vector[Byte])

/**
 * The RequestDeploy message indicates that a specific dataset needs to be deployed
 *
 * @param locator the internal identifier for the leased dataset
 * @param deployId an identifier assigned by the deployer service
 * @param server the details of the server that should host the deployment
 */
final case class RequestDeploy(locator: String, deployId: Long, server: Server)

/**
 * The RequestSimplify message indicates that the Simplify service should be invoked on a particular dataset.
 * This must be a vector dataset as simplify is a vector operation.
 *
 * @param locator the internal identifier for the dataset to be processed
 * @param tolerance the tolerance for the simplify operation
 */
final case class RequestSimplify(locator: String, tolerance: Double)

/**
 * JobRequest messages go to a job manager.
 */
sealed trait JobRequest

/**
 * The Submit message asks a job manager to queue a task for execution
 *
 * @param id a task identifier
 * @param service the name of the service to be executed
 * @param parameters parameters specifying the details of the action to be performed.  The service itself determines how these are interpreted.
 */
final case class Submit(id: String, service: String, parameters: Vector[String]) extends JobRequest

/**
 * The Report message notifies a job manager of a change in the execution status of a job
 *
 * @param status the new JobStatus (note that a jobstatus includes the id of the job, so this is not needed as a direct field of the Report message.)
 */
final case class Report(status: JobStatus) extends JobRequest

/**
 * A JobStatus represents a phase in the lifecyle of a managed job.
 */
sealed trait JobStatus {

  /** a unique identifier for the job whose status is indicated */
  def id: String
}

/**
 * Pending is the initial status for every job, no work has been done beyond registering the task in the system
 */
final case class Pending(id: String, serviceName: String, params: Vector[String]) extends JobStatus

/**
 * The Assigned status indicates that a worker node has been chosen to execute a task
 */
final case class Assigned(id: String, node: String) extends JobStatus

/**
 * The Started status indicates that execution has begun for a task.
 */
final case class Started(id: String, node: String) extends JobStatus

/**
 * The Progress status indicates some progress has been made in the execution of a task, indicated by a floating point value between 0 (no work done yet) and 1 (task complete.)
 * The granularity of the progress indication is up to the service, which may not report progress at all.
 * Services are not required to send a Progress of value 1 to indicate completion - the Done status is sent instead.
 */
final case class Progress(id: String, progress: Double) extends JobStatus

/**
 * The Done status indicates the task has completed successfully.
 *
 * @param result a string indicating the result, its interpretation is dependent on the service.  This might be a small JSON document or a dataset locator, for example.
 */
final case class Done(id: String, result: String) extends JobStatus

/**
 * The Failed status indicates that the task could not be completed normally, due to user or system error.
 */
final case class Failed(id: String, reason: String) extends JobStatus

/**
 * The Aborted status indicates that a job was cancelled due to external request.
 */
final case class Aborted(id: String) extends JobStatus
