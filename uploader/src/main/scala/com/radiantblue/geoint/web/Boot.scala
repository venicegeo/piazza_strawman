package com.radiantblue.geoint.web

import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

object Boot {
  def main(args: Array[String]): Unit = {
    type GeoIntService = akka.actor.Actor
    implicit val system = ActorSystem("on-spray-can")
    val service = system.actorOf(Props[GeoIntServiceActor], "geoint-service")
    implicit val timeout = Timeout(5.seconds)
    IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 8080)
  }
}
