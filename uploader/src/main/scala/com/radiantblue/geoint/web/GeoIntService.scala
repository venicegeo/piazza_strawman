package com.radiantblue.geoint.web

import com.radiantblue.geoint.Messages

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.Actor
import spray.routing._
import spray.http._, MediaTypes._
import spray.httpx.SprayJsonSupport._
import spray.httpx.unmarshalling._
import spray.httpx.marshalling._
import spray.json.DefaultJsonProtocol._

class GeoIntServiceActor extends Actor with GeoIntService {
  def futureContext = context.dispatcher
  def actorRefFactory = context
  def receive = runRoute(geointRoute)

  var kafkaProducer: kafka.javaapi.producer.Producer[String, Array[Byte]] = setupKafka
  var jdbcConnection: java.sql.Connection = setupJdbc

  def setupKafka = {
    val props = new java.util.Properties()
    props.put("zk.connect", "127.0.0.1:2181")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("metadata.broker.list", "127.0.0.1:9092")
    val config = new kafka.producer.ProducerConfig(props)
    new kafka.javaapi.producer.Producer[String, Array[Byte]](config)
  }

  def setupJdbc = {
    java.lang.Class.forName("org.postgresql.Driver")
    val props = new java.util.Properties()
    props.put("user", "geoint")
    props.put("password", "secret")
    java.sql.DriverManager.getConnection("jdbc:postgresql://localhost/metadata", props)
  }
}

trait GeoIntService extends HttpService {
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

  private val apiRoute =
    path("datasets") {
      get {
        parameters("keywords") { keywords =>
          complete(Future { 
            val pstmt = jdbcConnection.prepareStatement("SELECT name, checksum, size FROM metadata WHERE name LIKE ? ORDER BY id LIMIT 10;")
            pstmt.setString(1, s"%$keywords%")
            val results = pstmt.executeQuery()
            try {
              val iter = 
                Iterator.continually(results).takeWhile(_.next).map(rs => (rs.getString(1), rs.getString(2), rs.getLong(3)))

              import spray.json._
              val rows = iter.map { case (name, checksum, size) =>
                import spray.json._
                spray.json.JsObject(
                  "name" -> JsString(name),
                  "checksum" -> JsString(checksum),
                  "size" -> JsNumber(size)
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
          complete { Future {
            val buffs = data.entity.data.toByteString.asByteBuffers
            val path = java.nio.file.Files.createTempFile(
              java.nio.file.Paths.get("/tmp"), "geoint", "upload")
            val file = java.nio.file.Files.newByteChannel(path, java.nio.file.StandardOpenOption.WRITE)
            try {
              buffs.foreach { file.write(_) }
            } finally file.close()
            java.lang.Thread.sleep(500)
            fireUploadEvent(data.filename.getOrElse(""), path.toUri.toString)

            HttpResponse(status=StatusCodes.Found, headers=List(HttpHeaders.Location("/")))
          } }
        }
      }
    }

  def fireUploadEvent(filename: String, storageKey: String): Unit = {
    val upload = Messages.Upload.newBuilder().setName(filename).setLocator(storageKey).build()
    val message = new kafka.producer.KeyedMessage[String, Array[Byte]]("uploads", upload.toByteArray)
    kafkaProducer.send(message)
  }

  val geointRoute = pathPrefix("api")(apiRoute) ~ frontendRoute
}
