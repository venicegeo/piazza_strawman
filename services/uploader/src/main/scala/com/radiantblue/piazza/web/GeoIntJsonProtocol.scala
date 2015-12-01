package com.radiantblue.piazza.web

import spray.json._
import com.radiantblue.piazza.Messages.Server
import com.radiantblue.piazza.postgres.KeywordHit

trait PiazzaJsonProtocol extends DefaultJsonProtocol {
  implicit val keywordHitProtocol = jsonFormat7(KeywordHit)
  implicit val serverProtocol: RootJsonFormat[Server] =
    new RootJsonFormat[Server] {
      def read(json: JsValue): Server = {
        val map = json.convertTo[Map[String, JsValue]]
        Server.newBuilder()
          .setHost(map("address").convertTo[String])
          .setPort(map("port").convertTo[Int])
          .setLocalPath(map("localFilePath").convertTo[String])
          .build()
      }

      def write(server: Server): JsValue =
        Map(
          "address" -> server.getHost.toJson,
          "port" -> server.getPort.toJson,
          "localFilePath" -> server.getLocalPath.toJson)
        .toJson
    }
}
