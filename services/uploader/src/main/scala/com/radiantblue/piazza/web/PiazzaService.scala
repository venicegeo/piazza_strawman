package com.radiantblue.piazza.web

import com.radiantblue.deployer._
import com.radiantblue.piazza.{ kafka => _, _ }, postgres._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

import java.io._

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._


import akka.actor.{ Actor, ActorSystem }
import spray.routing._
import spray.http._
import spray.httpx.PlayTwirlSupport._
import spray.httpx.SprayJsonSupport._

import com.radiantblue.piazza.JsonProtocol._

class Attempt[T](attempt: => T) {
  private var result: Option[T] = None
  def get: T =
    result match {
      case Some(t) =>
        t
      case None =>
        synchronized {
          if (result == None) {
            result = Some(attempt)
          }
        }
        result.get
    }
  def optional: Option[T] = result
}

class PiazzaServiceActor extends Actor with PiazzaService {
  def actorSystem = context.system
  def futureContext = context.dispatcher
  def actorRefFactory = context
  def receive = runRoute(piazzaRoute)

  val kafkaProducer = com.radiantblue.piazza.kafka.Kafka.producer[String, Array[Byte]]()
  def jdbcConnection: java.sql.Connection = attemptJdbc.get

  private lazy val attemptKafka = new Attempt({
    com.radiantblue.piazza.kafka.Kafka.producer[String, Array[Byte]]()
  })

  private lazy val attemptJdbc = new Attempt({
    com.radiantblue.piazza.postgres.Postgres("piazza.metadata.postgres").connect()
  })

  // TODO: Hook actor shutdown to close connections using attempt.optional.foreach { _.close }
}

trait PiazzaService extends HttpService with PiazzaJsonProtocol {
  implicit def actorSystem: ActorSystem
  implicit def futureContext: ExecutionContext

  def kafkaProducer: kafka.producer.Producer[String, Array[Byte]]
  def jdbcConnection: java.sql.Connection

  private val frontendRoute =
    path("") {
      getFromResource("com/radiantblue/piazza/web/index.html")
    } ~
    path("index.html") {
      complete(HttpResponse(
        status = StatusCodes.MovedPermanently,
        headers = List(HttpHeaders.Location("/"))))
    } ~
    getFromResourceDirectory("com/radiantblue/piazza/web")

  private val datasetsApi =
    get {
      parameters("keywords") { keywords =>
        detach() {
          complete(
            Map("results" -> jdbcConnection.keywordSearch(keywords)))
        }
      }
    } ~
    post {
      // Adding Field for the PiazzaRequest payload
      formFields("jobId".as[String], "data".as[BodyPart]) { (jobId, data) =>
        detach() {
          // Firing existing message unchanged
          Deployer.withDeployer { dep =>
            val locator = dep.dataStore.store(data) // Add parameter here for JobID
            fireUploadEvent(data.filename.getOrElse(""), locator, jobId) 
            redirect("/", StatusCodes.Found)
          }
        }
      }
    }

  private val deploymentsApi =
    get {
      parameters('dataset, 'SERVICE.?, 'VERSION.?, 'REQUEST.?) { (dataset, service, version, request) =>
        detach() {
          (service, version, request) match {
            case (Some("WMS"), Some("1.3.0"), Some("GetCapabilities")) =>
              complete(
                  xml.wms_1_3_0(jdbcConnection.deploymentWithMetadata(dataset)))
            case (Some("WFS"), Some("1.0.0"), Some("GetCapabilities")) =>
              complete(
                  xml.wfs_1_0_0(jdbcConnection.deploymentWithMetadata(dataset)))
            case _ =>
              complete(
                  Map("servers" -> jdbcConnection.deployedServers(dataset)))
          }
        }
      }
    } ~
    post {
      rawPathPrefix(Slash) {
        (extract(_.request.uri) & formFields('dataset.?, 'jobId.?)) { (uri, dataset, jobId) =>
          val actualDataSet = dataset.orElse(jobId.map(jdbcConnection.jobIdSearch(_))).get
          onComplete(awaitDeployment(actualDataSet)) {
            case scala.util.Success(_) =>
              complete {
                val redirectTo = uri.withQuery(Uri.Query("dataset" -> actualDataSet))
                HttpResponse(
                  StatusCodes.Found,
                  headers = List(HttpHeaders.Location(redirectTo)))
              }
            case scala.util.Failure(_) =>
              complete {
                HttpResponse(
                  StatusCodes.BadRequest,
                  s"Cannot deploy dataset $dataset")
              }
          }
        }
      }
    }

  val parseLeaseGranted = fromJsonBytes[LeaseGranted]
  val leaseClient = new LeaseClient[Promise[Unit]](kafkaProducer)
  val listenF = Future { com.radiantblue.piazza.kafka.Kafka
    .consumer("uploader-lease-grants")
    .createMessageStreamsByFilter(kafka.consumer.Whitelist("lease-grants"))
    .map { stream =>
      stream.foreach { message =>
        try {
          val grant = parseLeaseGranted(message.message)
          val key: Array[Byte] = grant.tag.to[Array]
          val callback = leaseClient.retrieveContext(grant)
          for ((lease, promise) <- callback) {
            promise.success(())
            println(s"finished waiting on $lease")
          }
          callback.foreach { case (lease, promise) => promise.success(()) }
        } catch {
          case scala.util.control.NonFatal(ex) => ex.printStackTrace
        }
      }
    }
    ???
  }

  private def awaitDeployment(locator: String): Future[Unit] = {
    val promise = Promise[Unit]()
    leaseClient.requestLease(locator, 60 * 60 * 1000, promise)
    promise.future
  }

  private val apiRoute =
    pathPrefix("datasets") { datasetsApi } ~
    pathPrefix("deployments") { deploymentsApi }


  def fireUploadEvent(filename: String, storageKey: String, jobId: String): Unit = {
    val fw = new PrintWriter(new File("loggerfile" ))
    fw.write("file: " + filename + ", storageKey: " + storageKey + ", jobId: " + jobId + "\n")
    fw.flush()
    val format = toJsonBytes[Upload]
    val upload = Upload(
      name=filename,
      locator=storageKey,
      jobId)
    val formattedUpload = format(upload)
    /*val message = new kafka.producer.KeyedMessage[String, Array[Byte]]("uploads", formattedUpload)*/
    val message = new ProducerRecord[String, Array[Byte]]("uploads", formattedUpload);
    try {
      val kafkaProducerNew = com.radiantblue.piazza.kafka.Kafka.newProducer[String, Array[Byte]]()
      kafkaProducerNew.send(message)
    } catch {
      case ex: Exception => {
        fw.write("exception: " + ex)
        fw.flush()
      }
    }
    fw.write("Sent Kafka Message")
    fw.close()
  }

  val piazzaRoute = pathPrefix("api")(apiRoute) ~ frontendRoute
}

class LeaseClient[C](val kafkaProducer: kafka.producer.Producer[String, Array[Byte]]) {
  val format = toJsonBytes[RequestLease]
  var pending: Map[Seq[Byte], C] = Map.empty
  def requestLease(locator: String, timeout: Long, context: C): Unit = {
    val tag = leaseTag()
    synchronized {
      pending += (tag.toSeq -> context)
    }
    val request = RequestLease(
      locator=locator,
      timeout=timeout,
      tag=tag.to[Vector])
    sendToKafkaQueue("lease-requests", format(request))
  }

  def retrieveContext(grant: LeaseGranted): Option[(Lease, C)] = {
    val key = grant.tag.to[Array]
    val lease = Lease(0, 0, None, key) // TODO: Get lease details from Grant message
    val context = synchronized {
      val c = pending.get(key.toSeq)
      pending -= key.toSeq
      c
    }
    context.map(c => (lease, c))
  }

  private def leaseTag(): Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.putLong(scala.util.Random.nextLong())
    buffer.array()
  }

  private def sendToKafkaQueue(queue: String, value: Array[Byte]): Unit =
    kafkaProducer.send(new kafka.producer.KeyedMessage(queue, value))
}
