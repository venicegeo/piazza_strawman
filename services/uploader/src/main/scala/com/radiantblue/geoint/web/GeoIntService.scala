package com.radiantblue.geoint.web

import com.radiantblue.geoint.Messages
import com.radiantblue.deployer._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.{ Actor, ActorSystem }
import spray.routing._
import spray.http._, MediaTypes._
import spray.httpx.PlayTwirlSupport._
import spray.httpx.SprayJsonSupport._
import spray.httpx.unmarshalling._
import spray.httpx.marshalling._
import spray.json.DefaultJsonProtocol._

class Attempt[T](attempt: => T) {
  private var result: Option[T] = None
  def get: T = 
    result match {
      case Some(t) =>
        t
      case None => 
        synchronized {
          val x = attempt
          result = Some(x)
          x
        }
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
    val props = new java.util.Properties()
    props.put("zk.connect", "127.0.0.1:2181")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("metadata.broker.list", "127.0.0.1:9092")
    val config = new kafka.producer.ProducerConfig(props)
    new kafka.javaapi.producer.Producer[String, Array[Byte]](config)
  })

  private val attemptJdbc = new Attempt({
    java.lang.Class.forName("org.postgresql.Driver")
    val props = new java.util.Properties()
    props.put("user", "geoint")
    props.put("password", "secret")
    java.sql.DriverManager.getConnection("jdbc:postgresql://192.168.23.12/metadata", props)
  })

  // TODO: Hook actor shutdown to close connections using attempt.optional.foreach { _.close }
}

trait GeoIntService extends HttpService {
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
          val pstmt = jdbcConnection.prepareStatement(
            """
            SELECT 
              m.name,
              m.checksum,
              m.size,
              m.locator,
              gm.native_srid,
              ST_AsGeoJson(gm.latlon_bounds),
              d.server,
              d.deployed
            FROM metadata m 
              LEFT JOIN geometadata gm USING (locator)
              LEFT JOIN deployments d USING (locator)
            WHERE name LIKE ? ORDER BY m.id LIMIT 10
            """)
          pstmt.setString(1, s"%$keywords%")
          val results = pstmt.executeQuery()
          try {
            import spray.json._

            val iter = 
              Iterator.continually(results).takeWhile(_.next).map { rs => 
                (rs.getString(1),
                 rs.getString(2),
                 rs.getLong(3),
                 rs.getString(4),
                 Option(rs.getString(5)),
                 Option(rs.getString(6)).map(_.parseJson),
                 Option(rs.getString(7)),
                 rs.getBoolean(8))
              }

            val rows = iter.map { case (name, checksum, size, locator, native_srid, bbox, ds_server, ds_deployed) =>
              JsObject(
                "name" -> JsString(name),
                "checksum" -> JsString(checksum),
                "size" -> JsNumber(size),
                "locator" -> JsString(locator),
                "native_srid" -> native_srid.fold[JsValue](JsNull)(JsString(_)),
                "latlon_bbox" -> bbox.getOrElse(JsNull),
                "deployment_server" -> ds_server.filter(Function.const(ds_deployed)).fold[JsValue](JsNull)(JsString(_))
              )
            }
            JsObject("results" -> JsArray(rows.to[Vector]))
          } finally {
            results.close()
          }
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
                val pstmt = jdbcConnection.prepareStatement(
                  """
                  SELECT 
                    m.name,
                    m.checksum,
                    m.size,
                    m.locator,
                    gm.native_srid,
                    ST_XMin(gm.native_bounds),
                    ST_XMax(gm.native_bounds),
                    ST_YMin(gm.native_bounds),
                    ST_YMax(gm.native_bounds),
                    ST_XMin(gm.latlon_bounds),
                    ST_XMax(gm.latlon_bounds),
                    ST_YMin(gm.latlon_bounds),
                    ST_YMax(gm.latlon_bounds)
                  FROM metadata m 
                    JOIN geometadata gm USING (locator)
                    JOIN deployments d USING (locator)
                  WHERE d.deployed = TRUE 
                  AND locator = ?
                  """)
                try {
                  pstmt.setString(1, dataset)
                  val rs = pstmt.executeQuery()
                  try {
                    if (rs.next) {
                      val md = Messages.Metadata.newBuilder()
                        .setName(rs.getString(1))
                        .setChecksum(com.google.protobuf.ByteString.copyFrom(rs.getBytes(2)))
                        .setSize(rs.getLong(3))
                        .setLocator(rs.getString(4))
                        .build()
                      val geo = Messages.GeoMetadata.newBuilder()
                        .setLocator(rs.getString(4))
                        .setCrsCode(rs.getString(5))
                        .setNativeBoundingBox(Messages.GeoMetadata.BoundingBox.newBuilder()
                          .setMinX(rs.getDouble(6))
                          .setMaxX(rs.getDouble(7))
                          .setMinY(rs.getDouble(8))
                          .setMaxY(rs.getDouble(9))
                          .build())
                        .setLatitudeLongitudeBoundingBox(Messages.GeoMetadata.BoundingBox.newBuilder()
                          .setMinX(rs.getDouble(10))
                          .setMaxX(rs.getDouble(11))
                          .setMinY(rs.getDouble(12))
                          .setMaxY(rs.getDouble(13))
                          .build())
                        .build()
                      xml.wms_1_3_0(Vector((md, geo)))
                    } else {
                      xml.wms_1_3_0(Vector.empty)
                    }
                  } finally rs.close()
                } finally {
                  pstmt.close()
                }
              }
            }
          case _ =>
            complete { 
              Future {
                import spray.json._

                val statement = jdbcConnection.prepareStatement("SELECT server FROM deployments WHERE locator = ?")
                try {
                  statement.setString(1, dataset)
                  val results = statement.executeQuery()
                  try {
                    val iter = Iterator.continually(results).takeWhile(_.next).map { rs =>
                      rs.getString(1)
                    }
                    JsObject("servers" -> JsArray(iter.map(JsString(_)).to[Vector]))
                  } finally results.close()
                } finally {
                  statement.close()
                }
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
