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

import com.radiantblue.piazza._
import com.radiantblue.piazza.postgres._

import java.io._

/**
 * A MetadataStore is able to retrieve recorded metadata and geospatial metadata by locator string.
 */
trait MetadataStore {
  def lookup(locator: String): (Metadata, GeoMetadata)
}

/**
 * MetadataStore for Postgres databases
 */
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

/**
 * DatasetStorage for the local filesystem
 */
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

/**
 * Publish for GeoServer instances
 */
sealed class GeoServerPublish
  (sshUser: String, sshKey: java.nio.file.Path,
   geoserverUser: String, geoserverPassword: String,
   postgres: Postgres)
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
    geo.nativeFormat match {
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
        s"$sshUser@${server.host}:${server.localPath}")
      require(command.! == 0)
    }

    private def deleteFile(resource: java.nio.file.Path, server: Server): Future[Unit] = Future {
      import scala.sys.process._
      val key = sshKey.toAbsolutePath.toString
      val path = java.nio.file.Paths.get(server.localPath).resolve(resource.getFileName)
      val command = Vector(
        "ssh",
        "-oStrictHostKeyChecking=no",
        "-q",
        s"-i$key",
        s"${sshUser}@${server.host}",
        "rm",
        path.toAbsolutePath.toFile.toString)
      require(command.! == 0, s"Command $command failed")
    }

    def configure(md: Metadata, geo: GeoMetadata, server: Server): Future[Unit] = {
      val id = md.locator
      val serverUri: Uri = s"http://${server.host}:${server.port}/geoserver/rest/"
      val deleteUri = (s"workspaces/piazza/coveragestores/${id}?recurse=true": Uri) resolvedAgainst serverUri
      val storeUri = ("workspaces/piazza/coveragestores": Uri) resolvedAgainst serverUri
      val layerUri = (s"workspaces/piazza/coveragestores/${id}/coverages": Uri) resolvedAgainst serverUri
      for {
        deleteR <- pipeline(Delete(deleteUri))
        storeR <- pipeline(Post(storeUri,
          storeConfig(md.locator, md.name, md.locator)))
        _ <- Future { require(storeR.status.isSuccess, "Store creation failed") }
        layerR <- pipeline(Post(layerUri,
          layerConfig(
            md.name,
            md.locator,
            geo.nativeBoundingBox,
            geo.latitudeLongitudeBoundingBox,
            geo.crsCode)))
        _ <- Future { require(layerR.status.isSuccess, "Layer creation failed") }
      } yield ()
    }

    def unconfigure(md: Metadata, geo: GeoMetadata, server: Server): Future[Unit] = {
      val id = md.locator
      val serverUri: Uri = s"http://${server.host}:${server.port}/geoserver/rest/"
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

    def layerConfig(name: String, nativeName: String, nativeBbox: Bounds, latlonBbox: Bounds, srid: String): scala.xml.NodeSeq =
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
          <minx>{nativeBbox.minX}</minx>
          <maxx>{nativeBbox.maxX}</maxx>
          <miny>{nativeBbox.minY}</miny>
          <maxy>{nativeBbox.maxY}</maxy>
        </nativeBoundingBox>
        <latLonBoundingBox>
          <minx>{latlonBbox.minX}</minx>
          <maxx>{latlonBbox.maxX}</maxx>
          <miny>{latlonBbox.minY}</miny>
          <maxy>{latlonBbox.maxY}</maxy>
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
        "-nln", md.locator,
        "-nlt", "PROMOTE_TO_MULTI",
        s"PG:dbname='${database}' user='${user}' host='${host}' port='${port}' password='${password}'",
        shpPath.toFile.getAbsolutePath)
      require(command.! == 0)
    }

    def dropTable(md: Metadata, resource: java.nio.file.Path, server: Server): Future[Unit] = Future {
      val conn = postgres.connect()
      try {
        val query = conn.createStatement()
        query.execute(s"""DROP TABLE IF EXISTS "${md.locator}" CASCADE""");
        println("Dropped table " + md.locator)
      } catch {
        case scala.util.control.NonFatal(ex) =>
          ex.printStackTrace()
          throw ex
      } finally {
        conn.close()
      }
    }

    def configure(md: Metadata, geo: GeoMetadata, server: Server): Future[Unit] = {
      val id = md.locator
      val serverUri: Uri = s"http://${server.host}:${server.port}/geoserver/rest/"
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
      val id = md.locator
      val serverUri: Uri = s"http://${server.host}:${server.port}/geoserver/rest/"
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
        <name>{md.locator}</name>
        <nativeName>{md.locator}</nativeName>
        <title>{md.name}</title>
        <keywords>
          <string>features</string>
        </keywords>
        <nativeCRS>{ geo.crsCode }</nativeCRS>
        <srs>{ geo.crsCode }</srs>
        <nativeBoundingBox>
          <minx>{ geo.nativeBoundingBox.minX }</minx>
          <maxx>{ geo.nativeBoundingBox.maxX }</maxx>
          <miny>{ geo.nativeBoundingBox.minY }</miny>
          <maxy>{ geo.nativeBoundingBox.maxX }</maxy>
        </nativeBoundingBox>
        <latLonBoundingBox>
          <minx>{ geo.latitudeLongitudeBoundingBox.minX }</minx>
          <maxx>{ geo.latitudeLongitudeBoundingBox.maxX }</maxx>
          <miny>{ geo.latitudeLongitudeBoundingBox.minY }</miny>
          <maxy>{ geo.latitudeLongitudeBoundingBox.maxX }</maxy>
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
  /**
   * Request a deployment of the dataset with the given locator
   * If a live or pending deployment already exists it will be associated with
   * the lease instead of starting a new deployment.
   * 
   * @return a Future with the new Lease and the DeployStatus at the time the Lease is created
   */
  def attemptDeploy(locator: String, tag: Array[Byte]): Future[(Lease, DeployStatus)] = {
    Future(track.deploymentStatus(locator)).flatMap {
      case status @ Starting(token) =>
        for (lease <- Future(leasing.createLease(locator, token, tag))) yield (lease, status)
      case status @ Live(token, _) =>
        for (lease <- Future(leasing.attachLease(locator, token, tag))) yield (lease, status)
      case Dead =>
        for {
          token <- beginDeployment(locator)._1
          lease <- Future(leasing.attachLease(locator, token, tag))
        } yield (lease, Starting(token))
    }
  }

  /**
   * Request a new deployment of the dataset with the given locator
   *
   * @return a Future of the deployment id that completes as soon as the id is
   * assigned, and a Future of the server details that completes once the
   * deployment finishes
   */
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

  /**
   * Get the status of the deployment associated with a lease
   */
  def checkDeploy(leaseId: Long): Future[DeployStatus] =
    Future { leasing.checkDeploy(leaseId) }

  /**
   * Find and destroy all deployments with no unexpired leases.
   *
   * @return a Future that completes when all undeployments are done
   */
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

  /**
   * Use the [[http://stackoverflow.com/questions/20762240/loan-pattern-in-scala Loan pattern]] to create, use, and destroy a Deploy instance. 
   * Note that the Deploy instance is destroyed automatically after the passed
   * in function returns or throws, so returning any object that closes over
   * the Deploy will cause failures.
   */
  def withDeployer[T](f: Deploy[java.nio.file.Path] => T): T = {
    implicit val system: ActorSystem = ActorSystem("spray-client")
    import system.dispatcher

    try {
      val postgresConnection = Postgres("piazza.metadata.postgres").connect()
      try {
        f(deployer(postgresConnection))
      } finally postgresConnection.close()
    } finally system.shutdown()
  }

  private def deployer(postgresConnection: java.sql.Connection)(implicit system: ActorSystem, context: ExecutionContext) = {
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
          case Live(_, server) => println(s"Deployed to server ${server.host}:${server.port}")
          case Dead => println(s"Cannot deploy dataset with locator '$locator'")
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

/**
 * Executable object to cull the database.
 *
 * @see [[Deploy#cull]]
 */
object Cull {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("spray-client")
    import system.dispatcher

    val postgresConnection = Postgres("piazza.metadata.postgres").connect()
    val config = com.typesafe.config.ConfigFactory.load()

    Deployer.withDeployer { dep =>
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
