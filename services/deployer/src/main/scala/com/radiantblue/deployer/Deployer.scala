package com.radiantblue.deployer

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.sys.process._
import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import spray.can.Http
import spray.client.pipelining._
import spray.http._, HttpMethods._
import spray.httpx.marshalling.Marshaller

import com.radiantblue.geoint.Messages

sealed case class Server(address: String, port: String, localFilePath: String)

sealed trait DeployStatus[+S]
object Deploying extends DeployStatus[Nothing]
case class Deployed[S](server: S) extends DeployStatus[S]
object Undeployable extends DeployStatus[Nothing]

trait MetadataStore {
  def lookup(locator: String): Future[(Messages.Metadata, Messages.GeoMetadata)]
}

class PostgresMetadataStore(conn: java.sql.Connection)(implicit ec: ExecutionContext)
  extends MetadataStore
{
  def lookup(locator: String): Future[(Messages.Metadata, Messages.GeoMetadata)] =
    Future {
      val pstmt = conn.prepareStatement("""
        SELECT 
          m.name,
          m.checksum,
          m.size,
          gm.native_srid,
          ST_XMin(gm.native_bounds),
          ST_XMax(gm.native_bounds),
          ST_YMin(gm.native_bounds),
          ST_YMax(gm.native_bounds),
          ST_XMin(gm.latlon_bounds),
          ST_XMax(gm.latlon_bounds),
          ST_YMin(gm.latlon_bounds),
          ST_YMax(gm.latlon_bounds)
        FROM metadata m JOIN geometadata gm USING (locator) 
        WHERE locator = ?
        LIMIT 1
        """)
      try {
        pstmt.setString(1, locator)
        val results = pstmt.executeQuery()
        try {
          if (results.next) {
            val metadata = Messages.Metadata.newBuilder()
              .setName(results.getString(1))
              .setLocator(locator)
              .setChecksum(com.google.protobuf.ByteString.copyFrom(results.getBytes(2)))
              .setSize(results.getLong(3))
              .build()
            val geometadata = Messages.GeoMetadata.newBuilder()
              .setLocator(locator)
              .setCrsCode(results.getString(4))
              .setNativeBoundingBox(Messages.GeoMetadata.BoundingBox.newBuilder()
                .setMinX(results.getDouble(5))
                .setMaxX(results.getDouble(6))
                .setMinY(results.getDouble(7))
                .setMaxY(results.getDouble(8))
                .build())
              .setLatitudeLongitudeBoundingBox(Messages.GeoMetadata.BoundingBox.newBuilder()
                .setMinX(results.getDouble(9))
                .setMaxX(results.getDouble(10))
                .setMinY(results.getDouble(11))
                .setMaxY(results.getDouble(12))
                .build())
              .build()
            (metadata, geometadata)
          } else {
            sys.error(s"No geometadata found for $locator")
          }
        } finally results.close()
      } finally pstmt.close()
    }
}

/**
 * Abstraction over a dataset "cold" storage system that keeps big binary blobs
 * in some internal representation R, addressable by opaque string identifiers
 */
trait DatasetStorage[R] {
  def lookup(id: String): Future[R]
  def store(body: BodyPart): Future[String]
}

sealed class FileSystemDatasetStorage(prefix: String = "file:///tmp/")(implicit ec: ExecutionContext) extends DatasetStorage[java.nio.file.Path] {
  import java.nio.file.{ Files, Path, Paths }
  private val prefixPath = Paths.get(new java.net.URI(prefix))
  def lookup(id: String): Future[Path] =
    Future {
      val path = prefixPath.resolve(Paths.get(id))
      if (Files.exists(path)) {
        path
      } else {
        sys.error(s"$path does not exist (resolved from $id)")
      }
    }

  def store(body: BodyPart): Future[String] =
    Future {
      val buffs = body.entity.data.toByteString.asByteBuffers
      val path = Files.createTempFile(prefixPath, "geoint", "upload")
      val file = Files.newByteChannel(path, java.nio.file.StandardOpenOption.WRITE)
      try buffs.foreach(file.write(_))
      finally file.close()
      prefixPath.relativize(path).toString
    }
}

/**
 * The Provision trait encapsulates strategies for retrieving datasets from
 * long-term storage and preparing them for publishing via, for example, OGC
 * services in GeoServer.
 */
trait Provision[T, S] {
  def provision(dataset: T, server: S): Future[Unit]
  // unprovision??
}

sealed class OpenSSHProvision
  (sshUser: String, sshKey: java.nio.file.Path)
  (implicit ec: ExecutionContext)
  extends Provision[java.nio.file.Path, Server]
{ 
  import scala.sys.process._
  def provision(dataset: java.nio.file.Path, server: Server): Future[Unit] = Future {
    val key = sshKey.toAbsolutePath.toString
    val command = Vector(
      "rsync",
      "-e", s"ssh -oStrictHostKeyChecking=no -q -i$key",
      "--perms",
      "--chmod=u+rw,g+rw,o+r",
      dataset.toAbsolutePath.toString,
      s"$sshUser@${server.address}:${server.localFilePath}")
    if (command.! != 0) throw new Exception("Failed with non-zero exit status")
  }
}

/**
 * The Publish trait encapsulates strategies for exposing deployed resources
 * via services such as OGC WMS, WFS, and WCS.
 */
trait Publish[S] {
  def publish(metadata: Messages.Metadata, geo: Messages.GeoMetadata, server: S): Future[Unit]
}

sealed class GeoServerPublish
  (user: String, password: String)
  (implicit system: ActorSystem, ec: ExecutionContext) 
  extends Publish[Server]
{
  private implicit val timeout: Timeout = 5.seconds

  // Spray provides a marshaller for xml.NodeSeq, but we provide one here so
  // the MediaType will be `application/xml` as GeoServer expects, rather than
  // `text/xml` as is the Spray default
  private implicit val NodeSeqMarshaller =
    Marshaller.delegate[scala.xml.NodeSeq, String](MediaTypes.`application/xml`)(_.toString)

  private val pipeline = (
    addCredentials(BasicHttpCredentials(user: String, password: String))
    ~> sendReceive
  )

  def publish(md: Messages.Metadata, geo: Messages.GeoMetadata, server: Server): Future[Unit] = {
    val id = md.getLocator
    val serverUri: Uri = s"http://${server.address}:${server.port}/geoserver/rest/"
    val deleteUri = (s"workspaces/geoint/coveragestores/${id}?recurse=true": Uri) resolvedAgainst serverUri
    val storeUri = (s"workspaces/geoint/coveragestores": Uri) resolvedAgainst serverUri
    val layerUri = (s"workspaces/geoint/coveragestores/${id}/coverages": Uri) resolvedAgainst serverUri
    for {
      deleteR <- pipeline(Delete(deleteUri))
      storeR <- pipeline(Post(storeUri, 
        storeConfig(md.getLocator, md.getName, md.getLocator)))
      _ <- Future { require(storeR.status.isSuccess, "Store creation failed") }
      layerR <- pipeline(Post(layerUri,
        layerConfig(
          md.getName,
          md.getLocator,
          geo.getNativeBoundingBox,
          geo.getLatitudeLongitudeBoundingBox,
          geo.getCrsCode)))
      _ <- Future { require(layerR.status.isSuccess, "Layer creation failed") }
    } yield ()
  }

  private def storeConfig(name: String, title: String, file: String): scala.xml.NodeSeq =
    <coverageStore>
      <name>{name}</name>
      <description>{name}</description>
      <type>GeoTIFF</type>
      <enabled>true</enabled>
      <workspace>
        <name>geoint</name>
      </workspace>
      <url>file:data/{file}</url>
    </coverageStore>

  private def layerConfig(name: String, nativeName: String, nativeBbox: Messages.GeoMetadata.BoundingBox, latlonBbox: Messages.GeoMetadata.BoundingBox, srid: String): scala.xml.NodeSeq =
    <coverage>
      <name>{nativeName}</name>
      <nativeName>{nativeName}</nativeName>
      <namespace>
        <name>geoint</name>
      </namespace>
      <title>{name}</title>
      <description>Generated from GeoTIFF</description>
      <keywords>
        <string>WCS</string>
        <string>GeoTIFF</string>
        <string>{name}</string>
      </keywords>
      <srs>{srid}</srs>
      <nativeBoundingBox>
        <minx>{nativeBbox.getMinX}</minx>
        <maxx>{nativeBbox.getMaxX}</maxx>
        <miny>{nativeBbox.getMinY}</miny>
        <maxy>{nativeBbox.getMaxY}</maxy>
      </nativeBoundingBox>
      <latLonBoundingBox>
        <minx>{latlonBbox.getMinX}</minx>
        <maxx>{latlonBbox.getMaxX}</maxx>
        <miny>{latlonBbox.getMinY}</miny>
        <maxy>{latlonBbox.getMaxY}</maxy>
      </latLonBoundingBox>
      <projectionPolicy>REPROJECT_TO_DECLARED</projectionPolicy>
      <enabled>true</enabled>
      <metadata>
        <entry key="dirName">sfdem_sfdem</entry>
      </metadata>
      <store class="coverageStore">
        <name>sfdem</name>
      </store>
      <nativeFormat>GeoTIFF</nativeFormat>
      <defaultInterpolationMethod>nearest neighbor</defaultInterpolationMethod>
      <requestSRS>
        <string>{srid}</string>
      </requestSRS>
      <responseSRS>
        <string>{srid}</string>
      </responseSRS>
      <nativeCoverageName>{nativeName}</nativeCoverageName>
    </coverage>
}

/**
 * The Track trait encapsulates strategies for tracking resource deployments
 * with deployment tokens of type K
 */
trait Track[S, K] {
  def deploymentStarted(id: String): Future[(S, K)]
  def deploymentSucceeded(id: K): Future[Unit]
  def deploymentFailed(id: K): Future[Unit]
  def deploymentStatus(id: String): Future[DeployStatus[S]]
  def deployments(id: String): Future[Vector[S]]
}

sealed class PostgresTrack(conn: java.sql.Connection)(implicit ec: ExecutionContext) extends Track[Server, Long] {
  def deploymentStarted(id: String): Future[(Server, Long)] = 
    Future {
      val pstmt = conn.prepareStatement("INSERT INTO deployments (locator, server, deployed) VALUES (?, ?, false)",
        java.sql.Statement.RETURN_GENERATED_KEYS)
      try {
        val server = Server("192.168.23.13", "8081", "/var/lib/geoserver_data/geoserver1/data/")
        pstmt.setString(1, id)
        pstmt.setString(2, server.address)
        pstmt.executeUpdate()
        val results = pstmt.getGeneratedKeys()
        try {
          results.next
          val deployKey = results.getLong(1)
          (server, deployKey)
        } finally results.close()
      } finally pstmt.close()
    } 

  def deploymentSucceeded(id: Long): Future[Unit] = 
    Future {
      val pstmt = conn.prepareStatement("UPDATE deployments SET deployed = true WHERE id = ?");
      try {
        pstmt.setLong(1, id)
        pstmt.execute()
      } finally pstmt.close()
    } 

  def deploymentFailed(id: Long): Future[Unit] = 
    Future {
      val pstmt = conn.prepareStatement("DELETE FROM deployments WHERE id = ?");
      try     pstmt.execute()
      finally pstmt.close()
    }

  def deploymentStatus(id: String): Future[DeployStatus[Server]] = 
    Future {
      val pstmt = conn.prepareStatement("SELECT server, deployed FROM deployments WHERE locator = ?");
      try {
        pstmt.setString(1, id)
        val results = pstmt.executeQuery()
        try {
          if (results.next) {
            val deployed = results.getBoolean(2)
            if (deployed) {
              Deployed(Server(results.getString(1), "8081", "/var/lib/geoserver_data/geoserver1/data"))
            } else {
              Deploying
            }
          } else {
            Undeployable
          }
        } finally results.close()
      } finally pstmt.close()
    }

  def deployments(id: String): Future[Vector[Server]] = 
    Future {
      val pstmt = conn.prepareStatement("SELECT server FROM deployments WHERE locator = ? AND deployed = TRUE");
      try {
        pstmt.setString(1, id)
        val results = pstmt.executeQuery()
        try {
          Iterator
            .continually(results)
            .takeWhile(_.next)
            .map(rs => Server(rs.getString(1), "8081", "/var/lib/geoserver_data/geoserver1"))
            .to[Vector]
        } finally results.close()
      } finally pstmt.close()
    }
}

/**
 * The Provisioner[T,S] class handles the lifecycle of datasets of type T
 * provisioned to servers of type S and tracked with deployment keys of type K.
 */
sealed case class Deploy[D, S, K]
  (metadataStore: MetadataStore,
   dataStore: DatasetStorage[D],
   provision: Provision[D, S],
   publish: Publish[S],
   track: Track[S, K])
  (implicit ec: ExecutionContext)
{
  def attemptDeploy(locator: String): Future[DeployStatus[S]] = 
    track.deploymentStatus(locator).flatMap {
      case x @ (Deploying | Deployed(_)) => Future.successful(x)
      case Undeployable => 
        for {
          (metadata, geometadata) <- metadataStore.lookup(locator)
          resource <- dataStore.lookup(locator)
          (server, deploymentKey) <- track.deploymentStarted(locator)
          _ <- provision.provision(resource, server)
          _ <- publish.publish(metadata, geometadata, server)
          _ <- track.deploymentSucceeded(deploymentKey)
        } yield Deployed(server)
    }
}

object Deployer {
  private def storeConfig(name: String, title: String, file: String): scala.xml.NodeSeq = 
    <coverageStore>
      <name>{name}</name>
      <description>{name}</description>
      <type>GeoTIFF</type>
      <enabled>true</enabled>
      <workspace>
        <name>geoint</name>
      </workspace>
      <url>file:data/{file}</url>
    </coverageStore>

  private def layerConfig(name: String, nativeName: String, nativeBbox: Messages.GeoMetadata.BoundingBox, latlonBbox: Messages.GeoMetadata.BoundingBox, srid: String): scala.xml.NodeSeq =
    <coverage>
      <name>{nativeName}</name>
      <nativeName>{nativeName}</nativeName>
      <namespace>
        <name>geoint</name>
      </namespace>
      <title>{name}</title>
      <description>Generated from GeoTIFF</description>
      <keywords>
        <string>WCS</string>
        <string>GeoTIFF</string>
        <string>{name}</string>
      </keywords>
      <srs>{srid}</srs>
      <nativeBoundingBox>
        <minx>{nativeBbox.getMinX}</minx>
        <maxx>{nativeBbox.getMaxX}</maxx>
        <miny>{nativeBbox.getMinY}</miny>
        <maxy>{nativeBbox.getMaxY}</maxy>
      </nativeBoundingBox>
      <latLonBoundingBox>
        <minx>{latlonBbox.getMinX}</minx>
        <maxx>{latlonBbox.getMaxX}</maxx>
        <miny>{latlonBbox.getMinY}</miny>
        <maxy>{latlonBbox.getMaxY}</maxy>
      </latLonBoundingBox>
      <projectionPolicy>REPROJECT_TO_DECLARED</projectionPolicy>
      <enabled>true</enabled>
      <metadata>
        <entry key="dirName">sfdem_sfdem</entry>
      </metadata>
      <store class="coverageStore">
        <name>sfdem</name>
      </store>
      <nativeFormat>GeoTIFF</nativeFormat>
      <defaultInterpolationMethod>nearest neighbor</defaultInterpolationMethod>
      <requestSRS>
        <string>{srid}</string>
      </requestSRS>
      <responseSRS>
        <string>{srid}</string>
      </responseSRS>
      <nativeCoverageName>{nativeName}</nativeCoverageName>
    </coverage>

  def deploy(
    name: String,
    locator: String,
    nativeBbox: Messages.GeoMetadata.BoundingBox,
    latlonBbox: Messages.GeoMetadata.BoundingBox,
    srid: String)
    (implicit system: ActorSystem): Future[(StatusCode, StatusCode, StatusCode)] =
  {
    implicit val timeout: Timeout = 5.seconds
    implicit val context: ExecutionContext = system.dispatcher

    val geoserverIp = "192.168.23.13"
    val connectorF = 
      for (
        Http.HostConnectorInfo(connector, _) <- IO(Http) ? Http.HostConnectorSetup(geoserverIp, port=8081)
      ) yield connector

    val server: SendReceive =
      (r: HttpRequest) => connectorF.flatMap { conn =>
        val pipeline = sendReceive(conn)
        pipeline(r)
      }

    val pipeline = (
      addCredentials(BasicHttpCredentials("admin", "geoserver"))
      ~> server
    )

    val cleanedName = name.takeWhile('.' != _)
    val cleanedLocator = locator.reverse.takeWhile('/' != _).reverse
    val storeCfg = storeConfig(cleanedLocator, cleanedName, cleanedLocator)
    val layerCfg = layerConfig(cleanedName, cleanedLocator, nativeBbox, latlonBbox, srid)

    implicit val NodeSeqMarshaller =
        Marshaller.delegate[scala.xml.NodeSeq, String](MediaTypes.`application/xml`)(_.toString)

    val uploadCommand =
      List(
        "rsync",
        "-e", "ssh -oStrictHostKeyChecking=no -q -i/opt/deployer/geoserver-files",
        "--perms",
        "--chmod=u+rw,g+rw,o+r",
        java.nio.file.Paths.get(new java.net.URI(locator)).toAbsolutePath.toString,
        s"geoserver_files@$geoserverIp:/var/lib/geoserver_data/geoserver1/data/")

    val uploadF = 
      Future(uploadCommand.!).filter(_ == 0)

    for {
      uploadResult <- uploadF
      deleteResult <- pipeline(Delete("/geoserver/rest/workspaces/geoint/coveragestores/sfdem?recurse=true"))
      storeResult <- pipeline(Post("/geoserver/rest/workspaces/geoint/coveragestores", storeCfg))
      if storeResult.status.isSuccess
      coverageResult <- pipeline(Post("/geoserver/rest/workspaces/geoint/coveragestores/sfdem/coverages", layerCfg))
      if coverageResult.status.isSuccess
    } yield {
      (deleteResult.status, storeResult.status, coverageResult.status)
    }
  }

  def attemptDeploy
    (locator: String)
    (implicit system: ActorSystem): Future[DeployStatus[String]] = {
    Class.forName("org.postgresql.Driver")
    implicit val context: ExecutionContext = system.dispatcher
    val conn = {
      val props = new java.util.Properties
      props.put("user", "geoint")
      props.put("password", "secret")
      java.sql.DriverManager.getConnection("jdbc:postgresql://192.168.23.12/metadata", props)
    }

    val result = 
      Future(lookup(conn, locator)).flatMap { metadata =>
          metadata match {
            case Some((name, srid, nativeBounds, latlonBounds, None)) =>
              val id = startDeployment(conn, locator)
              for ((_, _, status) <- deploy(name, locator, nativeBounds, latlonBounds, srid)) yield
                if (status.isSuccess) {
                  finishDeployment(conn, id)
                  Deployed("192.168.23.13")
                } else {
                  failDeployment(conn, id)
                  Undeployable
                }
            case Some((_, _, _, _, Some(false))) =>
              Future.successful(Deploying)
            case Some((_, _, _, _, Some(true))) =>
              Future.successful(Deployed("192.168.23.13"))
            case None =>
              Future.successful(Undeployable)
          } 
        }

    result.onComplete { _ => conn.close() }

    result
  }

  private def lookup(conn: java.sql.Connection, locator: String): Option[(String, String, Messages.GeoMetadata.BoundingBox, Messages.GeoMetadata.BoundingBox, Option[Boolean])] = {
    val pstmt = conn.prepareStatement("""
      SELECT 
      m.name,
      gm.native_srid,
      ST_XMin(gm.native_bounds),
      ST_XMax(gm.native_bounds),
      ST_YMin(gm.native_bounds),
      ST_YMax(gm.native_bounds),
      ST_XMin(gm.latlon_bounds),
      ST_XMax(gm.latlon_bounds),
      ST_YMin(gm.latlon_bounds),
      ST_YMax(gm.latlon_bounds),
      d.deployed
      FROM metadata m 
      JOIN geometadata gm USING (locator) 
      LEFT JOIN deployments d USING (locator)
      WHERE locator = ?
      LIMIT 1
      """)
    try {
      pstmt.setString(1, locator)
      val results = pstmt.executeQuery()
      try {
        if (results.next)
          Some((
            results.getString(1),
            results.getString(2),
            (Messages.GeoMetadata.BoundingBox.newBuilder
              .setMinX(results.getDouble(3))
              .setMaxX(results.getDouble(4))
              .setMinY(results.getDouble(5))
              .setMaxY(results.getDouble(6))
              .build()),
            (Messages.GeoMetadata.BoundingBox.newBuilder
              .setMinX(results.getDouble(7))
              .setMaxX(results.getDouble(8))
              .setMinY(results.getDouble(9))
              .setMaxY(results.getDouble(10))
              .build()),
            Option(results.getObject(11)).collect { case b: java.lang.Boolean => b.booleanValue }
          ))
        else
          None
      } finally results.close()
    } finally pstmt.close()
  }

  def startDeployment(conn: java.sql.Connection, locator: String): Long = {
    val pstmt = conn.prepareStatement("INSERT INTO deployments (locator, server, deployed) VALUES (?, ?, false)",
      java.sql.Statement.RETURN_GENERATED_KEYS)
    try {
      pstmt.setString(1, locator)
      pstmt.setString(2, "192.168.23.13")
      pstmt.executeUpdate()
      val results = pstmt.getGeneratedKeys()
      try {
        results.next
        results.getLong(1)
      } finally results.close
    } finally pstmt.close
  }

  def finishDeployment(conn: java.sql.Connection, id: Long): Unit = {
    val pstmt = conn.prepareStatement("UPDATE deployments SET deployed = true WHERE id = ?");
    try {
      pstmt.setLong(1, id)
      pstmt.execute()
    } finally pstmt.close()
  }

  def failDeployment(conn: java.sql.Connection, id: Long): Unit = {
    val pstmt = conn.prepareStatement("DELETE FROM deployments WHERE id = ?");
    try     pstmt.execute()
    finally pstmt.close()
  }

  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")
    val locator = args(0)

    implicit val system: ActorSystem = ActorSystem("spray-client")
    import system.dispatcher

    val postgresConnection = {
      val props = new java.util.Properties()
      props.put("user", "geoint")
      props.put("password", "secret")
      java.sql.DriverManager.getConnection("jdbc:postgresql://192.168.23.12/metadata", props)
    }

    val deployer = 
      Deploy(
        new PostgresMetadataStore(postgresConnection),
        new FileSystemDatasetStorage(),
        new OpenSSHProvision("geoserver_files", java.nio.file.Paths.get("/opt/deployer/geoserver-files")),
        new GeoServerPublish("admin", "geoserver"),
        new PostgresTrack(postgresConnection))

    val printF = 
      for (result <- deployer.attemptDeploy(locator)) yield {
        result match {
          case Deploying => println("Deploying")
          case Deployed(_) => println("Deployed")
          case Undeployable => println(s"Cannot deploy dataset with locator '$locator'")
        }
      }

    printF.onComplete { x =>
      println(x)
      try
        postgresConnection.close()
      finally 
        system.shutdown()
    }
  }
}
