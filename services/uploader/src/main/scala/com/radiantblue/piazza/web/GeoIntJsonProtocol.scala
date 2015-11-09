package com.radiantblue.piazza.web

import spray.json._
import com.radiantblue.piazza.Server
import com.radiantblue.piazza.postgres.KeywordHit

trait PiazzaJsonProtocol extends DefaultJsonProtocol {
  implicit val keywordHitProtocol = jsonFormat7(KeywordHit)
  implicit val serverProtocol = jsonFormat3(Server)
}
