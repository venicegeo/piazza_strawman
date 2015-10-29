package com.radiantblue.geoint.web

import spray.json._
import com.radiantblue.geoint.postgres.KeywordHit

trait GeoIntJsonProtocol extends DefaultJsonProtocol {
  implicit val keywordHitProtocol = jsonFormat7(KeywordHit)
}
