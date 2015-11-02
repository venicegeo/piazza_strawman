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
import com.radiantblue.geoint.postgres._

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
    Future(conn.datasetWithMetadata(locator))
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
      val (server, token) = conn.startDeployment(id)
      (Server(server, "8081", "/var/lib/geoserver_data/geoserver1/data"), token)
    }

  def deploymentSucceeded(id: Long): Future[Unit] = 
    Future(conn.completeDeployment(id))

  def deploymentFailed(id: Long): Future[Unit] = 
    Future(conn.failDeployment(id))

  def deploymentStatus(id: String): Future[DeployStatus[Server]] = 
    Future {
      conn.getDeploymentStatus(id) match {
        case None => Undeployable
        case Some(None) => Deploying
        case Some(Some(host)) => 
          Deployed(Server(host, "8081", "/var/lib/geoserver_data/geoserver1/data"))
      }
    }

  def deployments(id: String): Future[Vector[Server]] = 
    Future {
      conn.deployedServers(id)
        .map(Server(_, "8081", "/var/lib/geoserver_data/geoserver1/data"))
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
          (server, token) <- track.deploymentStarted(locator)
          _ <- provision.provision(resource, server)
          _ <- publish.publish(metadata, geometadata, server)
          _ <- track.deploymentSucceeded(token)
        } yield Deployed(server)
    }
}

object Deployer {
  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")
    val locator = args(0)

    implicit val system: ActorSystem = ActorSystem("spray-client")
    import system.dispatcher

    val postgresConnection = Postgres.connect()
    val config = com.typesafe.config.ConfigFactory.load()

    val provisioner = {
      val geoserver = config.getConfig("geoint.geoserver")
      val sshUser = geoserver.getString("ssh.user")
      val sshKey = geoserver.getString("ssh.key")
      new OpenSSHProvision(sshUser, java.nio.file.Paths.get(sshKey))
    }
    val publisher = {
      val geoserver = config.getConfig("geoint.geoserver")
      val restUser = geoserver.getString("rest.user")
      val restPassword = geoserver.getString("rest.password")
      new GeoServerPublish(restUser, restPassword)
    }

    val deployer = 
      Deploy(
        new PostgresMetadataStore(postgresConnection),
        new FileSystemDatasetStorage(),
        provisioner,
        publisher,
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
