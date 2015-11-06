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

import com.radiantblue.piazza.Messages._
import com.radiantblue.piazza.postgres._

sealed case class Server(address: String, port: String, localFilePath: String)

sealed trait DeployStatus[+S]
object Deploying extends DeployStatus[Nothing]
case class Deployed[S](server: S) extends DeployStatus[S]
object Undeployable extends DeployStatus[Nothing]

trait MetadataStore {
  def lookup(locator: String): Future[(Metadata, GeoMetadata)]
}

class PostgresMetadataStore(conn: java.sql.Connection)(implicit ec: ExecutionContext)
  extends MetadataStore
{
  def lookup(locator: String): Future[(Metadata, GeoMetadata)] =
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
      val path = Files.createTempFile(prefixPath, "piazza", "upload")
      val file = Files.newByteChannel(path, java.nio.file.StandardOpenOption.WRITE)
      try buffs.foreach(file.write(_))
      finally file.close()
      prefixPath.relativize(path).toString
    }
}

/**
 * The Publish trait encapsulates strategies for exposing deployed resources
 * via services such as OGC WMS, WFS, and WCS.
 */
trait Publish[R,S] {
  def publish(metadata: Metadata, geo: GeoMetadata, resource: R, server: S): Future[Unit]
  def unpublish(metadata: Metadata, geo: GeoMetadata, resource: R, server: S): Future[Unit]
}

sealed class GeoServerPublish
  (sshUser: String, sshKey: java.nio.file.Path,
   geoserverUser: String, geoserverPassword: String,
   postgres: Postgres)
   // pgUser: String, pgPassword: String, pgHost: String, pgPort: Int, pgDatabase: String)
  (implicit system: ActorSystem, ec: ExecutionContext) 
  extends Publish[java.nio.file.Path, Server]
{
  private implicit val timeout: Timeout = 5.seconds

  // Spray provides a marshaller for xml.NodeSeq, but we provide one here so
  // the MediaType will be `application/xml` as GeoServer expects, rather than
  // `text/xml` as is the Spray default
  private implicit val NodeSeqMarshaller =
    Marshaller.delegate[scala.xml.NodeSeq, String](MediaTypes.`application/xml`)(_.toString)

  private val pipeline = (
    addCredentials(BasicHttpCredentials(geoserverUser: String, geoserverPassword: String))
    ~> sendReceive
  )

  def publish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Future[Unit] = {
    val datatype: Publish[java.nio.file.Path, Server] =
      geo.getNativeFormat match {
        case "geotiff" => Raster
        case "zipped-shapefile" => Feature
      }
    datatype.publish(md, geo, resource, server)
  }

  def unpublish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Future[Unit] = {
    val datatype: Publish[java.nio.file.Path, Server] =
      geo.getNativeFormat match {
        case "geotiff" => Raster
        case "zipped-shapefile" => Feature
      }
    datatype.unpublish(md, geo, resource, server)
  }

  private object Raster extends Publish[java.nio.file.Path, Server] {
    def publish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Future[Unit] = 
      for {
        _ <- copyFile(resource, server)
        _ <- configure(md, geo, server)
      } yield ()

    def unpublish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Future[Unit] =
      for {
        _ <- unconfigure(md, geo, server)
        _ <- deleteFile(resource, server)
      } yield ()

    private def copyFile(resource: java.nio.file.Path, server: Server): Future[Unit] = Future {
      import scala.sys.process._
      val key = sshKey.toAbsolutePath.toString
      val command = Vector(
        "rsync",
        "-e", s"ssh -oStrictHostKeyChecking=no -q -i$key",
        "--perms",
        "--chmod=u+rw,g+rw,o+r",
        resource.toAbsolutePath.toString,
        s"$sshUser@${server.address}:${server.localFilePath}")
      require(command.! == 0)
    }

    private def deleteFile(resource: java.nio.file.Path, server: Server): Future[Unit] = Future {
      import scala.sys.process._
      val key = sshKey.toAbsolutePath.toString
      val path = java.nio.file.Paths.get(server.localFilePath).resolve(resource.getFileName)
      val command = Vector(
        "ssh",
        "-oStrictHostKeyChecking=no",
        "-q",
        s"-i$key",
        "rm",
        path.toAbsolutePath.toFile.toString)
      require(command.! == 0)
    }

    def configure(md: Metadata, geo: GeoMetadata, server: Server): Future[Unit] = {
      val id = md.getLocator
      val serverUri: Uri = s"http://${server.address}:${server.port}/geoserver/rest/"
      val deleteUri = (s"workspaces/piazza/coveragestores/${id}?recurse=true": Uri) resolvedAgainst serverUri
      val storeUri = ("workspaces/piazza/coveragestores": Uri) resolvedAgainst serverUri
      val layerUri = (s"workspaces/piazza/coveragestores/${id}/coverages": Uri) resolvedAgainst serverUri
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

    def unconfigure(md: Metadata, geo: GeoMetadata, server: Server): Future[Unit] = {
      val id = md.getLocator
      val serverUri: Uri = s"http://${server.address}:${server.port}/geoserver/rest/"
      val deleteUri = (s"workspaces/piazza/coveragestores/${id}?recurse=true": Uri) resolvedAgainst serverUri
      for {
        deleteR <- pipeline(Delete(deleteUri))
        _ <- if (deleteR.status.isSuccess)
               Future.successful(())
             else
               Future.failed(new Exception("Coveragestore deletion failed: " + deleteR.entity.asString))
      } yield ()
    }

    def storeConfig(name: String, title: String, file: String): scala.xml.NodeSeq =
      <coverageStore>
        <name>{name}</name>
        <description>{name}</description>
        <type>GeoTIFF</type>
        <enabled>true</enabled>
        <workspace>
          <name>piazza</name>
        </workspace>
        <url>file:data/{file}</url>
      </coverageStore>

    def layerConfig(name: String, nativeName: String, nativeBbox: GeoMetadata.BoundingBox, latlonBbox: GeoMetadata.BoundingBox, srid: String): scala.xml.NodeSeq =
      <coverage>
        <name>{nativeName}</name>
        <nativeName>{nativeName}</nativeName>
        <namespace>
          <name>piazza</name>
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

  private object Feature extends Publish[java.nio.file.Path, Server] {
    def publish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Future[Unit] =
      for {
        _ <- copyTable(md, resource, server)
        _ <- configure(md, geo, server)
      } yield ()

    def unpublish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Future[Unit] = 
      for {
        _ <- unconfigure(md, geo, server)
        _ <- dropTable(md, resource, server)
      } yield ()

    def copyTable(md: Metadata, resource: java.nio.file.Path, server: Server): Future[Unit] = Future {
      import scala.collection.JavaConverters._
      import scala.sys.process._
      import GeoServerPublish.this.postgres._
      val workDir = java.nio.file.Files.createTempDirectory("unpack-zipped-shapefile")
      val zip = new java.util.zip.ZipFile(resource.toFile)
      val shp = zip.entries.asScala.map(_.getName).find(_.toLowerCase endsWith ".shp").get
      val shpPath = workDir.resolve(shp)
      for (e <- zip.entries.asScala) {
        val path = workDir.resolve(e.getName)
        val stream = zip.getInputStream(e)
        try java.nio.file.Files.copy(stream, path)
        finally stream.close()
      }
      val command = Vector(
        "ogr2ogr",
        "-f", "PostgreSQL",
        "-overwrite",
        "-nln", md.getLocator,
        "-nlt", "PROMOTE_TO_MULTI",
        s"PG:dbname='${database}' user='${user}' host='${host}' port='${port}' password='${password}'",
        shpPath.toFile.getAbsolutePath)
      require(command.! == 0)
    }

    def dropTable(md: Metadata, resource: java.nio.file.Path, server: Server): Future[Unit] = Future {
      val conn = postgres.connect()
      try {
        val query = conn.createStatement()
        query.execute(s"""DROP TABLE IF EXISTS "${md.getLocator}" CASCADE""");
      } finally {
        conn.close()
      }
    }

    def configure(md: Metadata, geo: GeoMetadata, server: Server): Future[Unit] = {
      val id = md.getLocator
      val serverUri: Uri = s"http://${server.address}:${server.port}/geoserver/rest/"
      val deleteUri = (s"workspaces/piazza/datastores/postgis/featuretypes/${id}?recurse=true": Uri) resolvedAgainst serverUri
      val layerUri = (s"workspaces/piazza/datastores/postgis/featuretypes": Uri) resolvedAgainst serverUri
      for {
        deleteR <- pipeline(Delete(deleteUri))
        layerR <- pipeline(Post(layerUri, layerConfig(md, geo)))
        _ <- if (layerR.status.isSuccess)
               Future.successful(())
             else 
               Future.failed(new Exception("Layer creation failed: " + layerR.entity.asString))
      } yield ()
    }

    def unconfigure(md: Metadata, geo: GeoMetadata, server: Server): Future[Unit] = {
      val id = md.getLocator
      val serverUri: Uri = s"http://${server.address}:${server.port}/geoserver/rest/"
      val deleteUri = (s"workspaces/paizza/datastores/postgis/featuretypes/${id}?recurse=true": Uri) resolvedAgainst serverUri
      for {
        deleteR <- pipeline(Delete(deleteUri))
        _ <- if (deleteR.status.isSuccess)
               Future.successful(())
             else 
               Future.failed(new Exception("Featuretype deletion failed: " + deleteR.entity.asString))
      } yield ()
    }

    def layerConfig(md: Metadata, geo: GeoMetadata): scala.xml.NodeSeq =
      <featureType>
        <name>{md.getLocator}</name>
        <nativeName>{md.getLocator}</nativeName>
        <title>{md.getName}</title>
        <keywords>
          <string>features</string>
          <string>{md.getName}</string>
        </keywords>
        <nativeCRS>{ geo.getCrsCode }</nativeCRS>
        <srs>{ geo.getCrsCode }</srs>
        <nativeBoundingBox>
          <minx>{ geo.getNativeBoundingBox.getMinX }</minx>
          <maxx>{ geo.getNativeBoundingBox.getMaxX }</maxx>
          <miny>{ geo.getNativeBoundingBox.getMinY }</miny>
          <maxy>{ geo.getNativeBoundingBox.getMaxX }</maxy>
        </nativeBoundingBox>
        <latLonBoundingBox>
          <minx>{ geo.getLatitudeLongitudeBoundingBox.getMinX }</minx>
          <maxx>{ geo.getLatitudeLongitudeBoundingBox.getMaxX }</maxx>
          <miny>{ geo.getLatitudeLongitudeBoundingBox.getMinY }</miny>
          <maxy>{ geo.getLatitudeLongitudeBoundingBox.getMaxX }</maxy>
        </latLonBoundingBox>
        <projectionPolicy>FORCE_DECLARED</projectionPolicy>
        <enabled>true</enabled>
        <maxFeatures>0</maxFeatures>
        <numDecimals>0</numDecimals>
        <overridingServiceSRS>false</overridingServiceSRS>
        <skipNumberMatched>false</skipNumberMatched>
        <circularArcPresent>false</circularArcPresent>
      </featureType>
  }
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
   // provision: Provision[D, S],
   publish: Publish[D, S],
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
          // _ <- provision.provision(metadata, geometadata, resource, server)
          _ <- publish.publish(metadata, geometadata, resource, server)
          _ <- track.deploymentSucceeded(token)
        } yield Deployed(server)
    }
}

object Deployer {
  def deployer(postgresConnection: java.sql.Connection)(implicit system: ActorSystem, context: ExecutionContext) = {
    val config = com.typesafe.config.ConfigFactory.load()

    val publisher = {
      val gs = config.getConfig("piazza.geoserver")
      val pg = Postgres("piazza.geodata.postgres")

      new GeoServerPublish(
        sshUser = gs.getString("ssh.user"),
        sshKey = java.nio.file.Paths.get(gs.getString("ssh.key")),
        geoserverUser = gs.getString("rest.user"),
        geoserverPassword = gs.getString("rest.password"),
        pg)
    }

    Deploy(
      new PostgresMetadataStore(postgresConnection),
      new FileSystemDatasetStorage(),
      publisher,
      new PostgresTrack(postgresConnection))
  }

  def main(args: Array[String]): Unit = {
    val locator = args(0)

    implicit val system: ActorSystem = ActorSystem("spray-client")
    import system.dispatcher

    val postgresConnection = Postgres("piazza.metadata.postgres").connect()
    val config = com.typesafe.config.ConfigFactory.load()

    val printF = 
      for (result <- deployer(postgresConnection).attemptDeploy(locator)) yield {
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
