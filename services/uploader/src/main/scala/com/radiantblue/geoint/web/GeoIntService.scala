package com.radiantblue.geoint.web

import com.radiantblue.geoint.Messages
import com.radiantblue.deployer._
import com.radiantblue.geoint.postgres._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.{ Actor, ActorSystem }
import spray.routing._
import spray.http._
import spray.httpx.PlayTwirlSupport._
import spray.httpx.SprayJsonSupport._

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

class GeoIntServiceActor extends Actor with GeoIntService {
  def actorSystem = context.system
  def futureContext = context.dispatcher
  def actorRefFactory = context
  def receive = runRoute(geointRoute)

  def kafkaProducer: kafka.javaapi.producer.Producer[String, Array[Byte]] = attemptKafka.get
  def jdbcConnection: java.sql.Connection = attemptJdbc.get

  private val attemptKafka = new Attempt({
    com.radiantblue.geoint.kafka.Kafka.producer[String, Array[Byte]]()
  })

  private val attemptJdbc = new Attempt({
    com.radiantblue.geoint.postgres.Postgres.connect()
  })

  // TODO: Hook actor shutdown to close connections using attempt.optional.foreach { _.close }
}

trait GeoIntService extends HttpService with GeoIntJsonProtocol {
  implicit def actorSystem: ActorSystem
  implicit def futureContext: ExecutionContext

  def kafkaProducer: kafka.javaapi.producer.Producer[String, Array[Byte]]
  def jdbcConnection: java.sql.Connection


  private val frontendRoute = 
    path("") {
      getFromResource("com/radiantblue/geoint/web/index.html")
    } ~
    path("index.html") {
      complete(HttpResponse(
        status = StatusCodes.MovedPermanently,
        headers = List(HttpHeaders.Location("/"))))
    } ~
    getFromResourceDirectory("com/radiantblue/geoint/web")

  private val datasetsApi = 
    get {
      parameters("keywords") { keywords =>
        complete(Future { 
          Map("results" -> jdbcConnection.keywordSearch(keywords))
        })
      }
    } ~
    post {
      formFields("data".as[BodyPart]) { data => 
        complete { 
          (new com.radiantblue.deployer.FileSystemDatasetStorage()).store(data).map { locator =>
            fireUploadEvent(data.filename.getOrElse(""), locator)
            HttpResponse(status=StatusCodes.Found, headers=List(HttpHeaders.Location("/")))
          }
        }
      }
    }

  private val deploymentsApi =
    get { 
      parameters('dataset, 'SERVICE.?, 'VERSION.?, 'REQUEST.?) { (dataset, service, version, request) =>
        (service, version, request) match {
          case (Some("WMS"), Some("1.3.0"), Some("GetCapabilities")) => 
            complete {
              Future {
                xml.wms_1_3_0(jdbcConnection.deploymentWithMetadata(dataset))
              }
            }
          case _ =>
            complete { 
              Future {
                Map("servers" -> jdbcConnection.deployedServers(dataset))
              }
            }
        }
      }
    } ~
    post {
      rawPathPrefix(Slash) {
        (extract(_.request.uri) & formField('dataset)) { (uri, dataset) =>
          complete {
            deployer.attemptDeploy(dataset).map { 
              case Deploying =>
                HttpResponse(StatusCodes.Accepted, "Deploying")
              case Deployed(server) => 
                val redirectTo = uri.withQuery(Uri.Query(("dataset", dataset)))
                HttpResponse(StatusCodes.Found, headers=List(HttpHeaders.Location(redirectTo)))
              case Undeployable =>
                HttpResponse(StatusCodes.BadRequest, s"Cannot deploy dataset with locator '$dataset'")
            }
          }
        }
      }
    }

  def deployer = 
    Deploy(
      new PostgresMetadataStore(jdbcConnection),
      new FileSystemDatasetStorage(),
      new OpenSSHProvision("geoserver_files", java.nio.file.Paths.get("/opt/deployer/geoserver-files")),
      new GeoServerPublish("admin", "geoserver"),
      new PostgresTrack(jdbcConnection))

  private val apiRoute =
    pathPrefix("datasets") { datasetsApi } ~ 
    pathPrefix("deployments") { deploymentsApi }

  def fireUploadEvent(filename: String, storageKey: String): Unit = {
    val upload = Messages.Upload.newBuilder().setName(filename).setLocator(storageKey).build()
    val message = new kafka.producer.KeyedMessage[String, Array[Byte]]("uploads", upload.toByteArray)
    kafkaProducer.send(message)
  }

  val geointRoute = pathPrefix("api")(apiRoute) ~ frontendRoute
}
