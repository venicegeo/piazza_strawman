package com.radiantblue.geoint

import com.radiantblue.geoint.Messages._
import spray.json._

package object postgres {
  final case class KeywordHit(
    name: String,
    checksum: String,
    size: Long,
    locator: String,
    nativeSrid: Option[String],
    latlonBbox: Option[JsValue],
    deploymentServer: Option[String])

  implicit class Queries(val conn: java.sql.Connection) extends AnyVal {
    private def prepare[T](sql: String)(f: java.sql.PreparedStatement => T): T = {
      val pstmt = conn.prepareStatement(sql)
      try {
        f(pstmt)
      } finally {
        pstmt.close()
      }
    }

    private def prepareWithGeneratedKeys[T](sql: String)(f: java.sql.PreparedStatement => T): T = {
      val pstmt = conn.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS)
      try {
        f(pstmt)
      } finally {
        pstmt.close()
      }
    }

    private def iterate[T](statement: java.sql.PreparedStatement)(f: java.sql.ResultSet => T): Vector[T] = {
      val results = statement.executeQuery()
      try {
        Iterator
          .continually(results)
          .takeWhile(_.next)
          .map(f)
          .to[Vector]
      } finally {
        results.close()
      }
    }

    private def iterateGeneratedKeys[T](statement: java.sql.PreparedStatement)(f: java.sql.ResultSet => T): Vector[T] = {
      statement.execute()
      val results = statement.getGeneratedKeys()
      try {
        Iterator
          .continually(results)
          .takeWhile(_.next)
          .map(f)
          .to[Vector]
      } finally {
        results.close()
      }
    }

    def keywordSearch(keyword: String): Vector[KeywordHit] = {
      val sql = """
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
      """
      prepare(sql) { ps =>
        ps.setString(1, s"%$keyword%")
        iterate(ps) { rs =>
         KeywordHit(
           name = rs.getString(1),
           checksum = rs.getString(2),
           size = rs.getLong(3),
           locator = rs.getString(4),
           nativeSrid = Option(rs.getString(5)),
           latlonBbox = Option(rs.getString(6)).map(_.parseJson),
           deploymentServer = Option(rs.getString(7)).filter(Function.const(rs.getBoolean(8))))
        }
      }
    }

    def datasetWithMetadata(locator: String): (Metadata, GeoMetadata) = {
      val sql = """
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
        LIMIT 2
      """
      prepare(sql) { ps =>
        ps.setString(1, locator)
        val result = iterate(ps) { rs =>
          val md = Metadata.newBuilder()
            .setName(rs.getString(1))
            .setChecksum(com.google.protobuf.ByteString.copyFrom(rs.getBytes(2)))
            .setSize(rs.getLong(3))
            .setLocator(locator)
            .build()
          val geo = GeoMetadata.newBuilder()
            .setLocator(locator)
            .setCrsCode(rs.getString(4))
            .setNativeBoundingBox(Messages.GeoMetadata.BoundingBox.newBuilder()
              .setMinX(rs.getDouble(5))
              .setMaxX(rs.getDouble(6))
              .setMinY(rs.getDouble(7))
              .setMaxY(rs.getDouble(8))
              .build())
            .setLatitudeLongitudeBoundingBox(Messages.GeoMetadata.BoundingBox.newBuilder()
              .setMinX(rs.getDouble(9))
              .setMaxX(rs.getDouble(10))
              .setMinY(rs.getDouble(11))
              .setMaxY(rs.getDouble(12))
              .build())
            .build()
          (md, geo)
        }
        result match {
          case Vector(r) => r
          case Vector() => sys.error(s"No geometadata found for $locator")
          case _ => sys.error(s"Multiple results found for $locator")
        }
      }
    }

    def deploymentWithMetadata(locator: String): Vector[(Metadata, GeoMetadata)] = {
      val sql = """
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
        FROM metadata m 
          JOIN geometadata gm USING (locator)
          JOIN deployments d USING (locator)
        WHERE d.deployed = TRUE 
        AND locator = ?
        LIMIT 1"""
      prepare(sql) { ps =>
        ps.setString(1, locator)
        iterate(ps)(rs => {
            val md = Metadata.newBuilder()
              .setName(rs.getString(1))
              .setChecksum(com.google.protobuf.ByteString.copyFrom(rs.getBytes(2)))
              .setSize(rs.getLong(3))
              .setLocator(locator)
              .build()
            val geo = GeoMetadata.newBuilder()
              .setLocator(locator)
              .setCrsCode(rs.getString(4))
              .setNativeBoundingBox(Messages.GeoMetadata.BoundingBox.newBuilder()
                .setMinX(rs.getDouble(5))
                .setMaxX(rs.getDouble(6))
                .setMinY(rs.getDouble(7))
                .setMaxY(rs.getDouble(8))
                .build())
              .setLatitudeLongitudeBoundingBox(Messages.GeoMetadata.BoundingBox.newBuilder()
                .setMinX(rs.getDouble(9))
                .setMaxX(rs.getDouble(10))
                .setMinY(rs.getDouble(11))
                .setMaxY(rs.getDouble(12))
                .build())
              .build()
            (md, geo)
        })
      }
    }

    def deployedServers(locator: String): Vector[String] = {
      val sql = 
        "SELECT server FROM deployments WHERE deployed = TRUE AND locator = ?"
      prepare(sql) { ps =>
        ps.setString(1, locator)
        iterate(ps) { _.getString(1) }
      }
    }

    def startDeployment(locator: String): (String, Long) = {
      val server = "192.168.23.13"
      val sql = "INSERT INTO deployments (locator, server, deployed) VALUES (?, ?, false)"
      prepareWithGeneratedKeys(sql) { ps =>
        ps.setString(1, locator)
        ps.setString(2, server)
        ps.executeUpdate()
        iterateGeneratedKeys(ps)(rs => (server, rs.getLong(1))).head 
      }
    }

    def completeDeployment(id: Long): Unit = {
      val sql = "UPDATE deployments SET deployed = TRUE WHERE id = ?"
      prepare(sql) { ps =>
        ps.setLong(1, id)
        ps.execute()
      }
    }

    def failDeployment(id: Long): Unit = {
      val sql = "DELETE FROM deployments WHERE id = ?"
      prepare(sql) { ps =>
        ps.setLong(1, id)
        ps.execute()
      }
    }

    def getDeploymentStatus(locator: String): Option[Option[String]] = {
      val sql = "SELECT server, deployed FROM deployments WHERE locator = ?"
      prepare(sql) { ps =>
        ps.setString(1, locator)
        iterate(ps)({rs =>
          val deployed = rs.getBoolean(2)
          if (deployed) 
            Some(rs.getString(1))
          else
            None
        }).headOption
      }
    }
  }
}
