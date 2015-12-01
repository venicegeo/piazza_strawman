package com.radiantblue.piazza.web

import com.radiantblue.piazza.Messages._
import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.collection.JavaConverters._

object Boot {
  val encodeJobRequest: JobRequest => (Option[String], Array[Byte]) =
    request => {
      val id =
        if (request.hasSubmit)
          request.getSubmit.getJobId
        else if (request.hasReport)
          request.getReport.getStatus.getId
        else
          throw new IllegalStateException(s"Unknown job request type $request")
      (Some(id), request.toByteArray)
    }

  val decodeJobRequest: kafka.message.MessageAndMetadata[String, Array[Byte]] => JobRequest =
    message => JobRequest.parseFrom(message.message)

  val encodeJobStatus: JobStatus => (Option[String], Array[Byte]) =
    request => (Some(request.getId), request.toByteArray)

  val decodeJobStatus: kafka.message.MessageAndMetadata[String, Array[Byte]] => JobStatus =
    message => JobStatus.parseFrom(message.message)

  def setupJobService() {
    val executorService: java.util.concurrent.ExecutorService = java.util.concurrent.Executors.newCachedThreadPool()
    val requests = Bus.onKafka[JobRequest](
      "job-request",
      encodeJobRequest,
      decodeJobRequest,
      executorService)
    val statuses = Bus.onKafka[JobStatus](
      "job-status",
      encodeJobStatus,
      decodeJobStatus,
      executorService)

    val simplify = {
      val request = Bus.onKafka[JobRequest](
        "simplify-job-request",
        encodeJobRequest,
        decodeJobRequest,
        executorService)
      val report = Bus.onKafka[JobStatus](
        "simplify-job-status",
        encodeJobStatus,
        decodeJobStatus,
        executorService)
      val worker = new SimplifyWorker(report)
      request.subscribe { jobRequest =>
        if (jobRequest.hasSubmit) {
          val t = jobRequest.getSubmit
          val task = Task(t.getServiceName, t.getParamsList.asScala.to[Vector])
          worker.submit(jobRequest.getSubmit.getJobId, task)
        }
      }
      report.subscribe(status => requests.post(JobLifecycle.report(status)))
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
