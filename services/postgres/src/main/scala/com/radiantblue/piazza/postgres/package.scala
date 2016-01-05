package com.radiantblue.piazza

import spray.json._

package object postgres {
  final case class KeywordHit(
    name: String,
    checksum: String,
    size: Long,
    locator: String,
    nativeSrid: Option[String],
    latlonBbox: Option[JsValue],
    deployed: Boolean)

  implicit class Queries(val conn: java.sql.Connection) extends AnyVal {
    private def prepare[T](sql: String, generatedKeys: Int = java.sql.Statement.NO_GENERATED_KEYS)(f: java.sql.PreparedStatement => T): T = {
      val pstmt = conn.prepareStatement(sql, generatedKeys)
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

    private def iterateKeys[T](statement: java.sql.PreparedStatement)(f: java.sql.ResultSet => T): Vector[T] = {
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

    def jobIdSearch(jobId: String): String = {
      val sql = """
      SELECT m.locator FROM metadata m WHERE m.jobId = ?
      """
      prepare(sql) { ps =>
        ps.setString(1, jobId)
        iterate(ps) { results =>
          results.getString(1)
        }.head
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
        (select bool_or(d.state = 'live') from deployments d where d.locator = m.locator)
      FROM metadata m
      LEFT JOIN geometadata gm USING (locator)
      WHERE name LIKE ?
      ORDER BY m.id
      LIMIT 10
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
           deployed = rs.getBoolean(7))
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
          ST_YMax(gm.latlon_bounds),
          gm.native_format
        FROM metadata m JOIN geometadata gm USING (locator)
        WHERE locator = ?
        LIMIT 2
      """
      prepare(sql) { ps =>
        ps.setString(1, locator)
        val result = iterate(ps) { rs =>
          val md = Metadata(
            name=rs.getString(1),
            checksum=rs.getBytes(2).to[Vector],
            size=rs.getLong(3),
            locator=locator)
          val geo = GeoMetadata(
            locator=locator,
            crsCode=rs.getString(4),
            nativeBoundingBox=Bounds(
              minX=rs.getDouble(5),
              maxX=rs.getDouble(6),
              minY=rs.getDouble(7),
              maxY=rs.getDouble(8)),
            latitudeLongitudeBoundingBox=Bounds(
              minX=rs.getDouble(9),
              maxX=rs.getDouble(10),
              minY=rs.getDouble(11),
              maxY=rs.getDouble(12)),
            nativeFormat=rs.getString(13))
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
          ST_YMax(gm.latlon_bounds),
          gm.native_format
        FROM metadata m
          JOIN geometadata gm USING (locator)
          JOIN deployments d USING (locator)
        WHERE d.state = 'live'
        AND locator = ?
        LIMIT 1"""
      prepare(sql) { ps =>
        ps.setString(1, locator)
        iterate(ps)(rs => {
            val md = Metadata(
              name=rs.getString(1),
              checksum=rs.getBytes(2).to[Vector],
              size=rs.getLong(3),
              locator=locator)
            val geo = GeoMetadata(
              locator=locator,
              crsCode=rs.getString(4),
              nativeBoundingBox=Bounds(
                minX=rs.getDouble(5),
                maxX=rs.getDouble(6),
                minY=rs.getDouble(7),
                maxY=rs.getDouble(8)),
              latitudeLongitudeBoundingBox=Bounds(
                minX=rs.getDouble(9),
                maxX=rs.getDouble(10),
                minY=rs.getDouble(11),
                maxY=rs.getDouble(12)),
              nativeFormat=rs.getString(13))
            (md, geo)
        })
      }
    }

    def deployedServers(locator: String): Vector[Server] = {
      val sql =
        """
        SELECT s.host, s.port, s.local_path
        FROM servers s
        JOIN deployments d ON (s.id = d.server)
        WHERE d.state = 'live' AND d.locator = ?
        """
      prepare(sql) { ps =>
        ps.setString(1, locator)
        iterate(ps) { rs =>
          Server(
            host=rs.getString(1),
            port=rs.getInt(2),
            localPath=rs.getString(3))
        }
      }
    }

    def timedOutServers(): Vector[(Long, String, Server)] = {
      val sql =
        """
        SELECT * FROM (
          SELECT
            d.id,
            d.locator,
            s.host,
            s.port,
            s.local_path,
            (SELECT max(l.lifetime) from leases l where l.deployment = d.id) lifetime
          FROM deployments d
          JOIN servers s
          ON (d.server = s.id)
          WHERE d.state = 'live')
        results WHERE lifetime < now()
        """
      prepare(sql) { ps =>
        iterate(ps) { rs => 
          val deployId = rs.getLong(1)
          val locator = rs.getString(2)
          val server = Server(
            host=rs.getString(3),
            port=rs.getInt(4),
            localPath=rs.getString(5))
          (deployId, locator, server)
        }
      }
    }

    def startDeployment(locator: String): (Server, Long) = {
      val sql =
        """
        INSERT INTO deployments (locator, server, state)
        SELECT ?, s.id, 'starting'
        FROM servers s
        ORDER BY response_time
        LIMIT 1
        RETURNING
          (SELECT host FROM servers WHERE servers.id = deployments.server),
          (SELECT port FROM servers WHERE servers.id = deployments.server),
          (SELECT local_path FROM servers WHERE servers.id = deployments.server),
          id
        """
      prepare(sql) { ps =>
        ps.setString(1, locator)
        iterate(ps)({ rs =>
          val server = Server(
            host=rs.getString(1),
            port=rs.getInt(2),
            localPath=rs.getString(3))
          val deployId = rs.getLong(4)
          (server, deployId)
        }).head
      }
    }

    def completeDeployment(id: Long): Unit = {
      val makeLive =
        """
        UPDATE deployments
        SET state = 'live'
        WHERE id = ?
        """
      val setTimeouts =
        """
        UPDATE leases SET lifetime = now() + '1 hour' WHERE lifetime IS NULL AND deployment = ?
        """
      prepare(makeLive) { ps =>
        ps.setLong(1, id)
        ps.execute()
      }
      prepare(setTimeouts) { ps =>
        ps.setLong(1, id)
        ps.execute()
      }
    }

    def failDeployment(id: Long): Unit = {
      val sql = "UPDATE deployments SET state = 'dead' WHERE id = ?"
      prepare(sql) { ps =>
        ps.setLong(1, id)
        ps.execute()
      }
    }

    def startUndeployment(id: Long): Unit = {
      val sql = "UPDATE deployments SET state = 'killing' WHERE id = ?"
      prepare(sql) { ps =>
        ps.setLong(1, id)
        ps.execute()
      }
    }

    def completeUndeployment(id: Long): Unit = {
      val sql = "UPDATE deployments SET state = 'dead' WHERE id = ?"
      prepare(sql) { ps =>
        ps.setLong(1, id)
        ps.execute()
      }
    }

    def failUndeployment(id: Long): Unit = {
      // maybe just don't worry about the difference between layers we decided
      // to delete and layers we successfully deleted?

      // val sql = "DELETE FROM deployments WHERE id = ?"
      // prepare(sql) { ps =>
      //   ps.setLong(1, id)
      //   ps.execute()
      // }
    }

    def getDeploymentStatus(locator: String): DeployStatus = {
      val sql =
        """
        SELECT d.state, d.id, s.host, s.port, s.local_path
        FROM deployments d
        JOIN servers s ON (d.server = s.id)
        WHERE d.locator = ?
        """
      prepare(sql) { ps =>
        ps.setString(1, locator)
        iterate(ps)({rs =>
          val state = rs.getString(1)
          val id = rs.getInt(2)

          state match {
            case "starting" =>
              Starting(id)
            case "live" =>
              val server = Server(
                host=rs.getString(3),
                port=rs.getInt(4),
                localPath=rs.getString(5))
              Live(id, server)
            case "dead" => Dead
          }
        }).headOption.getOrElse(Dead)
      }
    }

    def insertMetadata(md: Metadata): Unit = {
      val sql = "INSERT INTO metadata (name, locator, jobId, checksum, size) VALUES (?, ?, ?, ?, ?)"
      prepare(sql) { ps =>
        ps.setString(1, md.name)
        ps.setString(2, md.locator)
        ps.setString(3, md.jobId)
        ps.setString(4, md.checksum.map(b => f"$b%02x").mkString)
        ps.setLong(5, md.size)
        ps.executeUpdate()
      }
    }

    def insertGeoMetadata(g: GeoMetadata): Unit = {
      val sql = ("""
      INSERT INTO geometadata (locator, native_srid, native_bounds, latlon_bounds, native_format) VALUES (
        ?,
        ?,
        ST_MakeBox2D(ST_Point(?, ?), ST_Point(?, ?)),
        ST_MakeBox2D(ST_Point(?, ?), ST_Point(?, ?)),
        ?
      ) """)
      prepare(sql) { ps =>
        ps.setString(1, g.locator)
        ps.setString(2, g.crsCode)
        ps.setDouble(3, g.nativeBoundingBox.minX)
        ps.setDouble(4, g.nativeBoundingBox.minY)
        ps.setDouble(5, g.nativeBoundingBox.maxX)
        ps.setDouble(6, g.nativeBoundingBox.maxY)
        ps.setDouble(7, g.latitudeLongitudeBoundingBox.minX)
        ps.setDouble(8, g.latitudeLongitudeBoundingBox.minY)
        ps.setDouble(9, g.latitudeLongitudeBoundingBox.maxX)
        ps.setDouble(10, g.latitudeLongitudeBoundingBox.maxY)
        ps.setString(11, g.nativeFormat)
        ps.executeUpdate()
      }
    }

    def attachLease(locator: String, deployment: Long, tag: Array[Byte]): Lease = {
      val timeToLive = "1 hour";
      val sql =
        """
        INSERT INTO leases (locator, deployment, lifetime, tag) VALUES (?, ?, now() + (? :: INTERVAL), ?) RETURNING id, lifetime
        """
      prepare(sql) { ps =>
        ps.setString(1, locator)
        ps.setLong(2, deployment)
        ps.setString(3, timeToLive)
        ps.setBytes(4, tag)
        iterate(ps)({ rs => Lease(rs.getLong(1), deployment, Some(rs.getTimestamp(2)), tag) }).head
      }
    }

    def createLease(locator: String, deployment: Long, tag: Array[Byte]): Lease = {
      val sql =
        """
        INSERT INTO leases (locator, deployment, lifetime, tag) VALUES (?, ?, NULL, ?) RETURNING id
        """
      prepare(sql) { ps =>
        ps.setString(1, locator)
        ps.setLong(2, deployment)
        ps.setBytes(3, tag)
        iterate(ps)({ rs => Lease(rs.getLong(1), deployment, None, tag) }).head
      }
    }

    def setLeaseTime(id: Long, timeToLive: String): Unit = {
      val sql =
        """
        UPDATE leases SET lifetime = now() + (? :: INTERVAL) WHERE id = ?
        """
      prepare(sql) { ps =>
        ps.setString(1, timeToLive)
        ps.setLong(2, id)
        ps.execute()
      }
    }

    def getLeaseById(id: Int): (String, java.sql.Timestamp, Server) = {
      val sql =
        """
        SELECT l.locator, l.lifetime, s.host, s.port, s.local_path
        FROM leases l
        JOIN deployments d ON (l.deployment = d.id)
        JOIN servers s ON (d.server = s.id)
        WHERE l.id = id
        """
      prepare(sql) { ps =>
        ps.setInt(1, id)
        iterate(ps)({ rs =>
          val locator = rs.getString(1)
          val lifetime = rs.getTimestamp(2)
          val server = Server(
            host=rs.getString(3),
            port=rs.getInt(4),
            localPath=rs.getString(5))
          (locator, lifetime, server)
        }).head
      }
    }

    def checkLeaseDeployment(id: Long): DeployStatus = {
      val sql =
        """
        SELECT d.state, d.id, s.host, s.port, s.local_path
        FROM leases l
        JOIN deployments d ON (l.deployment = d.id)
        JOIN servers s ON (d.server = s.id)
        WHERE l.id = ?
        """
      prepare(sql) { ps =>
        ps.setLong(1, id)
        iterate(ps)({ rs =>
          val state = rs.getString(1)
          val id = rs.getInt(2)

          state match {
            case "starting" =>
              Starting(id)
            case "live" =>
              val server = Server(
                host=rs.getString(3),
                port=rs.getInt(4),
                localPath=rs.getString(5))
              Live(id, server)
            case "dead" => Dead
          }
        }).head
      }
    }

    def getLeasesByDeployment(id: Long): Vector[Lease] = {
      val sql = """ SELECT l.id, l.deployment, l.lifetime, l.tag FROM leases l WHERE l.deployment = ? """
      prepare(sql) { ps =>
        ps.setLong(1, id)
        iterate(ps) { rs =>
          Lease(rs.getLong(1), rs.getLong(2), Option(rs.getTimestamp(3)), rs.getBytes(4))
        }
      }
    }
  }
}
