package com.radiantblue.piazza

import spray.json._

/**
 * Json codecs for "core" message types in Piazza
 */
object JsonProtocol extends DefaultJsonProtocol {
  def toJsonBytes[T : JsonFormat]: T => Array[Byte] = t =>
    t.toJson.compactPrint.getBytes("utf-8")

  def fromJsonBytes[T : JsonFormat]: Array[Byte] => T = bytes =>
    new String(bytes, "utf-8").parseJson.convertTo[T]

  implicit val timestampFormat: JsonFormat[java.sql.Timestamp] =
    new JsonFormat[java.sql.Timestamp] {
      def read(json: JsValue): java.sql.Timestamp =
        java.sql.Timestamp.valueOf(json.convertTo[String])

      def write(timestamp: java.sql.Timestamp): JsValue =
        timestamp.toString.toJson
    }
  implicit val leaseFormat: RootJsonFormat[Lease] = jsonFormat4(Lease)
  implicit val uploadFormat: RootJsonFormat[Upload] = jsonFormat3(Upload)
  implicit val boundsFormat: RootJsonFormat[Bounds] = jsonFormat4(Bounds)
  implicit val metadataFormat: RootJsonFormat[Metadata] = jsonFormat5(Metadata)
  implicit val geoMetadataFormat: RootJsonFormat[GeoMetadata] = jsonFormat5(GeoMetadata)
  implicit val serverFormat: RootJsonFormat[Server] = jsonFormat3(Server)
  implicit val requestLeaseFormat: RootJsonFormat[RequestLease] = jsonFormat3(RequestLease)
  implicit val leaseGrantedFormat: RootJsonFormat[LeaseGranted] = jsonFormat3(LeaseGranted)
  implicit val requestDeployFormat: RootJsonFormat[RequestDeploy] = jsonFormat3(RequestDeploy)
  implicit val requestSimplifyFormat: RootJsonFormat[RequestSimplify] = jsonFormat2(RequestSimplify)
  // Patrick Messages
  implicit val piazzaRequestFormat: RootJsonFormat[PiazzaRequest] = jsonFormat2(PiazzaRequest)

  implicit val deployStatusFormat: RootJsonFormat[DeployStatus] = {
    implicit val startingFormat = jsonFormat1(Starting)
    implicit val liveFormat = jsonFormat2(Live)

    new RootJsonFormat[DeployStatus] {
      def read(json: JsValue): DeployStatus =
        json.asJsObject.fields("status").convertTo[String] match {
          case "starting" => json.convertTo[Starting]
          case "live" => json.convertTo[Live]
          case "dead" => Dead
        }

      def write(status: DeployStatus): JsValue = {
        val (tag, json) = status match {
          case s: Starting => ("starting", s.toJson)
          case l: Live => ("live", l.toJson)
          case Dead => ("dead", JsObject())
        }
        JsObject(json.asJsObject.fields + ("status" -> tag.toJson))
      }
    }
  }

  implicit val jobStatusFormat: RootJsonFormat[JobStatus] = {
    implicit val pendingFormat = jsonFormat3(Pending)
    implicit val assignedFormat = jsonFormat2(Assigned)
    implicit val startedFormat = jsonFormat2(Started)
    implicit val progressFormat = jsonFormat2(Progress)
    implicit val doneFormat = jsonFormat2(Done)
    implicit val failedFormat = jsonFormat2(Failed)
    implicit val abortedFormat = jsonFormat1(Aborted)

    new RootJsonFormat[JobStatus] {
      def read(json: JsValue): JobStatus =
        json.asJsObject.fields("status").convertTo[String] match {
          case "pending" => json.convertTo[Pending]
          case "assigned" => json.convertTo[Assigned]
          case "started" => json.convertTo[Started]
          case "progress" => json.convertTo[Progress]
          case "done" => json.convertTo[Done]
          case "failed" => json.convertTo[Failed]
          case "aborted" => json.convertTo[Aborted]
        }

      def write(status: JobStatus): JsValue = {
        val (tag, json) = status match {
          case p: Pending => ("pending", p.toJson)
          case a: Assigned => ("assigned", a.toJson)
          case s: Started => ("started", s.toJson)
          case p: Progress => ("progress", p.toJson)
          case d: Done => ("done", d.toJson)
          case f: Failed => ("failed", f.toJson)
          case a: Aborted => ("aborted", a.toJson)
        }
        JsObject(json.asJsObject.fields + ("status" -> tag.toJson))
      }
    }
  }

  implicit val jobRequestFormat: RootJsonFormat[JobRequest] = {
    implicit val submitFormat = jsonFormat3(Submit)
    implicit val reportFormat = jsonFormat1(Report)

    new RootJsonFormat[JobRequest] {
      def read(json: JsValue): JobRequest =
        json.asJsObject.fields("type") match {
          case JsString("submit") => json.convertTo[Submit]
          case JsString("report") => json.convertTo[Report]
          case _ => throw new DeserializationException("")
        }
      def write(request: JobRequest): JsValue = {
        val (tag, jsobject) =
          request match {
            case s: Submit => ("submit", s.toJson)
            case r: Report => ("report", r.toJson)
          }

        JsObject(jsobject.asJsObject.fields + ("tag" -> tag.toJson))
      }
    }
  }
}
