package com.radiantblue.deployer

import scala.concurrent.Future
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

object Deployer {
  private def storeConfig(name: String, file: String): scala.xml.NodeSeq = 
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
      <name>{name}</name>
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

  def deploy(name: String, locator: String, nativeBbox: Messages.GeoMetadata.BoundingBox, latlonBbox: Messages.GeoMetadata.BoundingBox, srid: String): Unit = {
    implicit val timeout: Timeout = 5.seconds
    implicit val system: ActorSystem = ActorSystem("spray-client")
    import system.dispatcher

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
    val storeCfg = storeConfig(cleanedName, cleanedLocator)
    val layerCfg = layerConfig(cleanedName, cleanedLocator, nativeBbox, latlonBbox, srid)

    implicit val NodeSeqMarshaller =
        Marshaller.delegate[scala.xml.NodeSeq, String](MediaTypes.`application/xml`)(_.toString)

    val uploadCommand =
      List(
        "rsync",
        "-e", "ssh -oStrictHostKeyChecking=no -q -i/opt/deployer/geoserver-files",
        "--perms",
        "--chmod=u+rw,g+rw,o+r",
        locator.drop("file://".length),
        s"geoserver_files@$geoserverIp:/var/lib/geoserver_data/geoserver1/data/$cleanedLocator")

    val uploadF = 
      Future(uploadCommand.!).filter(_ == 0)

    val resultF = for {
      _ <- uploadF
      deleteResult <- pipeline(Delete("/geoserver/rest/workspaces/geoint/coveragestores/sfdem?recurse=true"))
      _ = println(deleteResult)
      storeResult <- pipeline(Post("/geoserver/rest/workspaces/geoint/coveragestores", storeCfg))
      _ = println(storeResult)
      if storeResult.status.isSuccess
      coverageResult <- pipeline(Post("/geoserver/rest/workspaces/geoint/coveragestores/sfdem/coverages", layerCfg))
      _ = println(coverageResult)
      if coverageResult.status.isSuccess
    } yield {
      (deleteResult.status, storeResult.status, coverageResult.status)
    }
    
    resultF.onComplete { x =>
      println(x)
      system.shutdown()
    }
  }

  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")
    val locator = args(0)

    val conn = {
      val props = new java.util.Properties
      props.put("user", "geoint")
      props.put("password", "secret")
      java.sql.DriverManager.getConnection("jdbc:postgresql://192.168.23.12/metadata", props)
    }

    val record = 
      try {
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
              ST_YMax(gm.latlon_bounds)
          FROM metadata m JOIN geometadata gm 
          USING (locator) 
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
                    .build())
                ))
              else
                None
          } finally results.close()
        } finally pstmt.close()
      } finally conn.close()

    record match {
      case Some((name, srid, nativeBounds, latlonBounds)) => 
        deploy(name, locator, nativeBounds, latlonBounds, srid)
      case None => println(s"No dataset found for locator ${args(0)}")
    }
  }
}
