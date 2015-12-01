package com.radiantblue.piazza.web

import akka.actor.Actor
import scala.collection.JavaConverters._
import spray.routing._
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json._
import com.radiantblue.piazza.Messages._

class JobServiceActor(
  val requests: Bus[JobRequest],
  val statuses: Bus[JobStatus],
  val registry: Map[String, (Bus[JobRequest], Bus[JobStatus])]
) extends Actor with JobService {
  import JobLifecycle.report
  def actorRefFactory = context
  def executionContext = context.dispatcher
  val manager = new MemoryManager(requests, statuses, registry)
  statuses.subscribe(println(_))
  def receive = runRoute(jobRoute)
}

trait JobService extends HttpService with JobJsonProtocol {
  import JobLifecycle.submit
  implicit def executionContext: scala.concurrent.ExecutionContext
  def requests: Bus[JobRequest]
  def statuses: Bus[JobStatus]
  def manager: Manager

  def jobRoute: Route =
    pathPrefix("jobs") {
      pathPrefix(Segment) { jobId =>
        get {
          complete(manager.check(jobId))
        }
      } ~
      post {
        entity(as[Task]) { task =>
          detach() {
            val jobId = manager.nextId
            requests.post(submit(jobId, task))
            redirect(s"/jobs/$jobId", StatusCodes.Found)
          }
        }
      }
    }
}

trait JobJsonProtocol extends DefaultJsonProtocol {
  import JobLifecycle.{ pending, assigned, started, progress, done, failed, aborted }
  implicit val statusFormat: RootJsonFormat[JobStatus] = new RootJsonFormat[JobStatus] {
    def read(json: JsValue): JobStatus =
      json.asJsObject.getFields("status", "id") match {
        case Seq(JsString("pending"), JsString(id)) =>
          val Seq(task) = json.asJsObject.getFields("task")
          pending(id, task.convertTo[Task])
        case Seq(JsString("assigned"), JsString(id)) => assigned(id)
        case Seq(JsString("started"), JsString(id)) => started(id)
        case Seq(JsString("progress"), JsString(id)) => progress(id)
        case Seq(JsString("done"), JsString(id)) =>
          val Seq(result) = json.asJsObject.getFields("result")
          done(id, result.convertTo[String])
        case Seq(JsString("failed"), JsString(id)) => failed(id)
        case Seq(JsString("aborted"), JsString(id)) => aborted(id)
        case _ => throw new DeserializationException("not a valid status")
      }

    def write(status: JobStatus): JsValue =
      if (status.hasPending) {
        val task = Task(status.getPending.getServiceName, status.getPending.getParamsList.asScala.to[Vector])
        JsObject(
          "id" -> JsString(status.getId),
          "status" -> JsString("pending"),
          "task" -> task.toJson)
      } else if (status.hasAssigned) {
        JsObject(
          "id" -> JsString(status.getId),
          "status" -> JsString("assigned"))
      } else if (status.hasStarted) {
        JsObject(
          "id" -> JsString(status.getId),
          "status" -> JsString("started"))
      } else if (status.hasProgress) {
        JsObject(
          "id" -> JsString(status.getId),
          "status" -> JsString("progress"))
      } else if (status.hasDone) {
        JsObject(
          "id" -> JsString(status.getId),
          "status" -> JsString("done"),
          "result" -> JsString(status.getDone.getResult))
      } else if (status.hasFailed) {
        JsObject(
          "id" -> JsString(status.getId),
          "status" -> JsString("failed"))
      } else if (status.hasAborted) {
        JsObject(
          "id" -> JsString(status.getId),
          "status" -> JsString("aborted"))
      } else {
        throw new IllegalStateException(s"Unsupported JobStatusstatus")
      }
  }

  implicit val taskFormat: RootJsonFormat[Task] = jsonFormat2(Task)
}

sealed case class Task(service: String, parameters: Vector[String])

object JobLifecycle {
  def canAdvanceTo(oldStatus: JobStatus, newStatus: JobStatus): Boolean = {
    if (oldStatus.getId != newStatus.getId)
      false
    else if (oldStatus.hasPending)
      newStatus.hasAssigned || newStatus.hasAborted
    else if (oldStatus.hasAssigned)
      newStatus.hasStarted || newStatus.hasAborted
    else if (oldStatus.hasStarted)
      newStatus.hasProgress || newStatus.hasDone || newStatus.hasFailed
    else if (oldStatus.hasProgress)
      newStatus.hasProgress || newStatus.hasDone || newStatus.hasFailed
    else false
  }

  def pending(id: String, task: Task): JobStatus =
    JobStatus.newBuilder()
      .setPending(JobStatus.Pending.newBuilder()
        .setServiceName(task.service)
        .addAllParams(task.parameters.asJava)
        .build())
      .setId(id)
      .build()

  def assigned(id: String): JobStatus =
    JobStatus.newBuilder()
      .setAssigned(JobStatus.Assigned.newBuilder()
        .setNode("")
        .build())
      .setId(id)
      .build()

  def started(id: String): JobStatus =
    JobStatus.newBuilder()
      .setStarted(JobStatus.Started.newBuilder()
        .setNode("")
        .build())
      .setId(id)
      .build()

  def progress(id: String): JobStatus =
    JobStatus.newBuilder()
      .setProgress(JobStatus.Progress.newBuilder()
        .setProgress(.5f)
        .build())
      .setId(id)
      .build()

  def done(id: String, result: String): JobStatus =
    JobStatus.newBuilder()
      .setDone(JobStatus.Done.newBuilder()
        .setResult(result)
        .build())
      .setId(id)
      .build()

  def failed(id: String): JobStatus =
    JobStatus.newBuilder()
      .setFailed(JobStatus.Failed.newBuilder()
        .setReason("Bye")
        .build())
      .setId(id)
      .build()

  def aborted(id: String): JobStatus =
    JobStatus.newBuilder()
      .setAborted(JobStatus.Aborted.newBuilder()
        .build())
      .setId(id)
      .build()


  def submit(id: String, task: Task): JobRequest =
    JobRequest.newBuilder()
      .setSubmit(JobRequest.Submit.newBuilder()
        .setJobId(id)
        .setServiceName(task.service)
        .addAllParams(task.parameters.asJava)
        .build())
      .build()

  def report(status: JobStatus): JobRequest =
    JobRequest.newBuilder()
      .setReport(JobRequest.Report.newBuilder()
        .setStatus(status)
        .build())
      .build()
}

trait Bus[M] {
  def post(message: M): Unit
  def subscribe(onMessage: M => Unit): Unit
}

object Bus {
  private var worker = java.util.concurrent.Executors.newCachedThreadPool()
  def of[M]: Bus[M] =
    new Bus[M] {
      private var subscribers = Vector.empty[M => Unit]

      def post(message: M): Unit = subscribers.foreach { sub =>
        worker.submit(new java.util.concurrent.Callable[Unit] {
          def call() = sub(message)
        })
      }

      def subscribe(onMessage: M => Unit): Unit =
        subscribers :+= onMessage
    }

  def onKafka[M](queueName: String,
    encode: M => (Option[String], Array[Byte]),
    decode: kafka.message.MessageAndMetadata[String, Array[Byte]] => M,
    executorService: java.util.concurrent.ExecutorService)
  : Bus[M] =
    new Bus[M] {
      private var subscribers = Vector.empty[M => Unit]
      private var producer: kafka.producer.Producer[String, Array[Byte]] =
        com.radiantblue.piazza.kafka.Kafka.producer("key.serializer.class" -> "kafka.serializer.StringEncoder")

      {
        val connector: kafka.consumer.ConsumerConnector =
          com.radiantblue.piazza.kafka.Kafka.consumer(queueName)
        connector
          .createMessageStreamsByFilter(
            kafka.consumer.Whitelist(queueName),
            keyDecoder = new kafka.serializer.StringDecoder)
          .foreach { stream =>
            executorService.submit(new java.util.concurrent.Callable[Unit] {
              def call() = {
                for (message <- stream) {
                  val m = decode(message)
                  subscribers.foreach { sub =>
                    executorService.submit(new java.util.concurrent.Callable[Unit] {
                      def call() = sub(m)
                    })
                  }
                }
              }
            })
          }
      }

      def post(m: M) = {
        val (key, message) = encode(m)
        val keyedMessage = new kafka.producer.KeyedMessage[String, Array[Byte]](queueName, key.orNull, message)
        producer.send(keyedMessage)
      }

      def subscribe(handler: M => Unit): Unit =
        subscribers :+= handler
    }
}

trait Manager {
  def check(jobId: String): JobStatus
  def nextId(): String
}

final class MemoryManager(
  requests: Bus[JobRequest],
  statuses: Bus[JobStatus],
  registry: Map[String, (Bus[JobRequest], Bus[JobStatus])])
extends Manager {
  import JobLifecycle.pending
  private var counter = 0
  private var state = Map.empty[String, JobStatus]
  private var issuedIds = Set.empty[String]
  requests.subscribe(request)

  def nextId(): String = synchronized {
    counter += 1
    issuedIds += counter.toString
    counter.toString
  }

  private def request(r: JobRequest): Unit =
    synchronized {
      if (r.hasSubmit) {
        val id = r.getSubmit.getJobId
        val task = Task(r.getSubmit.getServiceName, r.getSubmit.getParamsList.asScala.to[Vector])
        if (issuedIds(id)) {
          val (req, stat) = registry(task.service)
          req.post(r)
          val status = pending(id, task)
          state += (id -> status)
          issuedIds -= id
          statuses.post(status)
        }
      } else if (r.hasReport) {
        val newStatus = r.getReport.getStatus
        for (oldStatus <- state.get(newStatus.getId) if JobLifecycle.canAdvanceTo(oldStatus, newStatus)) {
          state += (newStatus.getId -> newStatus)
          statuses.post(newStatus)
        }
      }
    }

  def check(jobId: String): JobStatus = state(jobId)
}

trait Router {
  def bus[T <: com.google.protobuf.GeneratedMessage]
    (name: String)
    (decode: Array[Byte] => T)
  : Bus[T]

  def keyedBus[T <: com.google.protobuf.GeneratedMessage]
    (name: String)
    (key: T => String, decode: Array[Byte] => T)
  : Bus[T]
}

class InProcessRouter {
  private var lookup = Map.empty[String, Bus[_]]

  def bus[T <: com.google.protobuf.GeneratedMessage]
    (name: String)
    (decode: Array[Byte] => T)
  : Bus[T] =
    lookup.get(name).map(_.asInstanceOf[Bus[T]]).getOrElse {
      val bus = Bus.of[T]
      lookup += (name -> bus)
      bus
    }

  def keyedBus[T <: com.google.protobuf.GeneratedMessage]
    (name: String)
    (key: T => String, decode: Array[Byte] => T)
  : Bus[T]
  = bus(name)(decode)
}

trait Worker {
  def submit(id: String, task: Task): Unit
}

final class SimplifyWorker(statuses: Bus[JobStatus]) extends Worker {
  import JobLifecycle.{ assigned, started, progress, done }
  def submit(id: String, task: Task): Unit = {
    try {
      statuses.post(assigned(id))
      statuses.post(started(id))
      Thread.sleep(5000) // Pretend to do a lot of work
      statuses.post(progress(id))
      Thread.sleep(5000) // Continue work
      statuses.post(done(id, "hi"))
    } catch {
      case scala.util.control.NonFatal(_) =>
        val message = JobStatus.newBuilder
          .setFailed(JobStatus.Failed.newBuilder.build())
          .setId(id)
          .build()
        statuses.post(message)
    }
  }
}
