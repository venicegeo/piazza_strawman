package com.radiantblue.geoint.web

import com.radiantblue.geoint.Messages

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.Actor
import spray.routing._
import spray.http._, MediaTypes._

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

              val rows = iter.map { case (name, checksum, size) =>
                <tr><td>{name}</td><td>{checksum}</td><td>{size}</td></tr>
              }
              <html>
                <body>
                  <h2> Search results </h2>
                  <a href="/">Back to home page.</a>
                  <table>
                    <tr><th>Name</th><th>Checksum</th><th>Size</th></tr>
                    { rows }
                  </table>
                </body>
              </html>
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

            <html>
              <body>
                <h2>Upload completed.</h2>
                <a href="/">Back to home page.</a>
              </body>
            </html>
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
