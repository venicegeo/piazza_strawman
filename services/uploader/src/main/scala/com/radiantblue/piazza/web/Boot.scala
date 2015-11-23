package com.radiantblue.piazza.web

import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

object Boot {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("on-spray-can")
    implicit val timeout = Timeout(5.seconds)

    val service = system.actorOf(Props(new JobServiceActor), "job-service")
    IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 8080)
  }
}
