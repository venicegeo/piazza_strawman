package com.radiantblue {
  package object deployer {
    import scala.concurrent.{ ExecutionContext, Future }

    implicit class NoisyFuture[T](val f: Future[T]) extends AnyVal {
      def noisy(implicit ec: ExecutionContext): Future[T] = {
        f.map { t => println(t); t }
      }
    }
  }
}

package com.radiantblue.deployer {

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

import com.radiantblue.piazza._, Messages._, postgres._

trait MetadataStore {
  def lookup(locator: String): (Metadata, GeoMetadata)
}

class PostgresMetadataStore(conn: java.sql.Connection)(implicit ec: ExecutionContext)
  extends MetadataStore
{
  def lookup(locator: String): (Metadata, GeoMetadata) =
    conn.datasetWithMetadata(locator)
}

/**
 * Abstraction over a dataset "cold" storage system that keeps big binary blobs
 * in some internal representation R, addressable by opaque string identifiers
 */
trait DatasetStorage[R] {
  def lookup(id: String): R
  def store(body: BodyPart): String
}

sealed class FileSystemDatasetStorage(prefix: String = "file:///tmp/") extends DatasetStorage[java.nio.file.Path] {
  import java.nio.file.{ Files, Path, Paths }
  private val prefixPath = Paths.get(new java.net.URI(prefix))
  def lookup(id: String): Path = {
    val path = prefixPath.resolve(Paths.get(id))
    if (Files.exists(path)) {
      path
    } else {
      sys.error(s"$path does not exist (resolved from $id)")
    }
  }

  def store(body: BodyPart): String = {
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
  def publish(metadata: Metadata, geo: GeoMetadata, resource: R, server: S): Unit
  def unpublish(metadata: Metadata, geo: GeoMetadata, resource: R, server: S): Unit
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

  def resolvePublisher(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Publish[java.nio.file.Path, Server] =
    geo.getNativeFormat match {
      case "geotiff" => Raster
      case "zipped-shapefile" => Feature
    }

  def publish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Unit =
    resolvePublisher(md, geo, resource, server)
      .publish(md, geo, resource, server)

  def unpublish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Unit =
    resolvePublisher(md, geo, resource, server)
      .unpublish(md, geo, resource, server)

  private def await[T](f: Future[T]): T = Await.result(f, Duration.Inf)

  private object Raster extends Publish[java.nio.file.Path, Server] {
    def publish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Unit = 
      await {
        for {
          _ <- copyFile(resource, server)
          _ <- configure(md, geo, server)
        } yield ()
      }

    def unpublish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Unit =
      await {
        for {
          _ <- unconfigure(md, geo, server)
          _ <- deleteFile(resource, server)
        } yield ()
      }

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
        s"${sshUser}@${server.address}",
        "rm",
        path.toAbsolutePath.toFile.toString)
      require(command.! == 0, s"Command $command failed")
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
    def publish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Unit =
      await {
        for {
          _ <- copyTable(md, resource, server)
          _ <- configure(md, geo, server)
        } yield ()
      }

    def unpublish(md: Metadata, geo: GeoMetadata, resource: java.nio.file.Path, server: Server): Unit = 
      await {
        for {
          _ <- unconfigure(md, geo, server)
          _ <- dropTable(md, resource, server)
        } yield ()
      }

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
        println("Dropped table " + md.getLocator)
      } catch {
        case scala.util.control.NonFatal(ex) =>
          ex.printStackTrace()
          throw ex
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
      val deleteUri = (s"workspaces/piazza/datastores/postgis/featuretypes/${id}?recurse=true": Uri) resolvedAgainst serverUri
      for {
        deleteR <- pipeline(Delete(deleteUri))
        _ = println(deleteR)
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
  def deploymentStarted(id: String): (S, K)
  def deploymentSucceeded(id: K): Unit
  def deploymentFailed(id: K): Unit
  def undeploymentStarted(id: K): Unit
  def undeploymentSucceeded(id: K): Unit
  def undeploymentFailed(id: K): Unit
  def deploymentStatus(locator: String): DeployStatus
  def deployments(id: String): Vector[S]
  def timedOutDeployments(): Vector[(Long, String, S)]
}

sealed class PostgresTrack(conn: java.sql.Connection)(implicit ec: ExecutionContext) extends Track[Server, Long] {
  def deploymentStarted(id: String): (Server, Long) = 
    conn.startDeployment(id)

  def deploymentSucceeded(id: Long): Unit = 
    conn.completeDeployment(id)

  def deploymentFailed(id: Long): Unit = 
    conn.failDeployment(id)

  def undeploymentStarted(id: Long): Unit =
    conn.startUndeployment(id)

  def undeploymentSucceeded(id: Long): Unit =
    conn.completeUndeployment(id)

  def undeploymentFailed(id: Long): Unit =
    conn.failUndeployment(id)

  def deploymentStatus(id: String): DeployStatus = 
    conn.getDeploymentStatus(id)

  def deployments(id: String): Vector[Server] = 
    conn.deployedServers(id)

  def timedOutDeployments(): Vector[(Long, String, Server)] =
    conn.timedOutServers()
}

trait Leasing {
  def createLease(locator: String, deployToken: Long, tag: Array[Byte]): Lease
  def attachLease(locator: String, deployToken: Long, tag: Array[Byte]): Lease
  def checkDeploy(id: Long): DeployStatus
}

sealed class PostgresLeasing(conn: java.sql.Connection)(implicit ec: ExecutionContext) extends Leasing {
  def createLease(locator: String, deployToken: Long, tag: Array[Byte]): Lease =
    conn.createLease(locator, deployToken, tag)

  def attachLease(locator: String, deployToken: Long, tag: Array[Byte]): Lease =
    conn.attachLease(locator, deployToken, tag)

  def checkDeploy(id: Long): DeployStatus =
    conn.checkLeaseDeployment(id)
}

/**
 * The Provisioner[T,S] class handles the lifecycle of datasets of type T
 * provisioned to servers of type S and tracked with deployment keys of type K.
 */
sealed case class Deploy[D]
  (metadataStore: MetadataStore,
   dataStore: DatasetStorage[D],
   publish: Publish[D, com.radiantblue.piazza.Server],
   leasing: Leasing,
   track: Track[com.radiantblue.piazza.Server, Long])
  (implicit ec: scala.concurrent.ExecutionContext)
{
  def attemptDeploy(locator: String, tag: Array[Byte]): Future[(Lease, DeployStatus)] = {
    Future(track.deploymentStatus(locator)).flatMap {
      case status @ Starting(token) =>
        for (lease <- Future(leasing.createLease(locator, token, tag))) yield (lease, status)
      case status @ Live(token, _) =>
        for (lease <- Future(leasing.attachLease(locator, token, tag))) yield (lease, status)
      case Killing | Dead => 
        for {
          token <- beginDeployment(locator)._1
          lease <- Future(leasing.attachLease(locator, token, tag))
        } yield (lease, Starting(token))
    }
  }

  def beginDeployment(locator: String): (Future[Long], Future[Server]) = {
    val deploymentInfo = Future(track.deploymentStarted(locator))
    val deployment = 
      for {
        (server, token) <- deploymentInfo
        (metadata, geometadata) <- Future(metadataStore.lookup(locator))
        resource <- Future(dataStore.lookup(locator))
        _ <- Future(publish.publish(metadata, geometadata, resource, server))
        _ <- Future(track.deploymentSucceeded(token))
      } yield server
    (deploymentInfo.map(_._2), deployment)
  }

  def checkDeploy(leaseId: Long): Future[DeployStatus] =
    Future { leasing.checkDeploy(leaseId) }

  def cull(): Future[Unit] = 
    Future(track.timedOutDeployments).flatMap { deployments =>
      Future.sequence {
        for ((token, locator, server) <- deployments) yield
          for {
            (md, geo) <- Future(metadataStore.lookup(locator))
            resource <- Future(dataStore.lookup(locator))
            _ <- Future(track.undeploymentStarted(token))
            _ <- Future(publish.unpublish(md, geo, resource, server))
            _ <- Future(track.undeploymentSucceeded(token))
          } yield ()
      }.map(_ => ())
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
      metadataStore = new PostgresMetadataStore(postgresConnection),
      dataStore = new FileSystemDatasetStorage(),
      publish = publisher,
      leasing = new PostgresLeasing(postgresConnection),
      track = new PostgresTrack(postgresConnection))
  }

  def main(args: Array[String]): Unit = {
    val locator = args(0)

    implicit val system: ActorSystem = ActorSystem("spray-client")
    import system.dispatcher

    val postgresConnection = Postgres("piazza.metadata.postgres").connect()
    val config = com.typesafe.config.ConfigFactory.load()

    val tag = {
      val buff = java.nio.ByteBuffer.allocate(8)
      buff.putLong(scala.util.Random.nextLong)
      buff.array
    }
    val printF = 
      for (result <- deployer(postgresConnection).attemptDeploy(locator, tag)) yield {
        result._2 match {
          case Starting(_) => println("Deployment in progress")
          case Live(_, server) => println(s"Deployed to server ${server.address}:${server.port}")
          case Dead | Killing => println(s"Cannot deploy dataset with locator '$locator'")
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

object Cull {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("spray-client")
    import system.dispatcher

    val postgresConnection = Postgres("piazza.metadata.postgres").connect()
    val config = com.typesafe.config.ConfigFactory.load()

    val dep = Deployer.deployer(postgresConnection)
    dep.cull().onComplete { _ =>
      println("Done")
      try
        postgresConnection.close()
      finally
        system.shutdown()
    }
  }
}
}
