package com.radiantblue.piazza.web

import com.radiantblue.piazza.{ kafka => _, _ }
import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.collection.JavaConverters._

import spray.json._
import com.radiantblue.piazza.JsonProtocol._

object Boot {
  def setupJobService() {
    val executorService: java.util.concurrent.ExecutorService = java.util.concurrent.Executors.newCachedThreadPool()
    val requests = Bus.onKafka[JobRequest](
      "job-request",
      toJsonBytes[JobRequest],
      fromJsonBytes[JobRequest],
      executorService)
    val statuses = Bus.onKafka[JobStatus](
      "job-status",
      toJsonBytes[JobStatus],
      fromJsonBytes[JobStatus],
      executorService)

    val simplify = {
      val request = Bus.onKafka[JobRequest](
        "simplify-job-request",
        toJsonBytes[JobRequest],
        fromJsonBytes[JobRequest],
        executorService)
      val report = Bus.onKafka[JobStatus](
        "simplify-job-status",
        toJsonBytes[JobStatus],
        fromJsonBytes[JobStatus],
        executorService)
      val worker = new SimplifyWorker(report)
      request.subscribe { jobRequest =>
        jobRequest match {
          case Submit(id, service, parameters) =>
            val task = Task(service, parameters)
            worker.submit(id, task)
          case _ =>
        }
      }
      report.subscribe(status => requests.post(Report(status)))
      (request, report)
    }
    val registry = Map("simplify" -> simplify)

    Props(new JobServiceActor(requests, statuses, registry))
  }

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("on-spray-can")
    implicit val timeout = Timeout(5.seconds)

    val service = system.actorOf(Props[PiazzaServiceActor], "piazza-service")
    IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 8080)
  }
}
