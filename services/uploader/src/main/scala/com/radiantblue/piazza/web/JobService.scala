package com.radiantblue.piazza.web

import akka.actor.Actor
import scala.collection.JavaConverters._
import spray.routing._
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json._
import com.radiantblue.piazza.{ kafka => _, _ }

class JobServiceActor(
  val requests: Bus[JobRequest],
  val statuses: Bus[JobStatus],
  val registry: Map[String, (Bus[JobRequest], Bus[JobStatus])]
) extends Actor with JobService {
  def actorRefFactory = context
  def executionContext = context.dispatcher
  val manager = new MemoryManager(requests, statuses, registry)
  statuses.subscribe(println(_))
  def receive = runRoute(jobRoute)
}

trait JobService extends HttpService with JobJsonProtocol {
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
            requests.post(Submit(jobId, task.service, task.parameters))
            redirect(s"/jobs/$jobId", StatusCodes.Found)
          }
        }
      }
    }
}

trait JobJsonProtocol extends DefaultJsonProtocol {
  implicit val statusFormat: RootJsonFormat[JobStatus] = new RootJsonFormat[JobStatus] {
    def read(json: JsValue): JobStatus = {
      val obj = json.asJsObject
      obj.getFields("status", "id") match {
        case Seq(JsString("pending"), JsString(id)) =>
          val task = obj.fields("task").convertTo[Task]
          Pending(id, task.service, task.parameters)
        case Seq(JsString("assigned"), JsString(id)) =>
          val node = obj.fields("node").convertTo[String]
          Assigned(id, node)
        case Seq(JsString("started"), JsString(id)) =>
          val node = obj.fields("node").convertTo[String]
          Started(id, node)
        case Seq(JsString("progress"), JsString(id)) =>
          val progress = obj.fields("progress").convertTo[Float]
          Progress(id, progress)
        case Seq(JsString("done"), JsString(id)) =>
          val result = obj.fields("result").convertTo[String]
          Done(id, result)
        case Seq(JsString("failed"), JsString(id)) =>
          val reason = obj.fields("reason").convertTo[String]
          Failed(id, reason)
        case Seq(JsString("aborted"), JsString(id)) =>
          Aborted(id)
        case _ => throw new DeserializationException("not a valid status")
      }
    }

    def write(status: JobStatus): JsValue =
      status match {
        case Pending(id, serviceName, parameters) =>
          JsObject(
            "id" -> id.toJson,
            "status" -> "pending".toJson,
            "task" -> Task(serviceName, parameters).toJson)
        case Assigned(id, node) =>
          JsObject(
            "id" -> id.toJson,
            "status" -> "assigned".toJson,
            "node" -> node.toJson)
        case Started(id, node) =>
          JsObject(
            "id" -> id.toJson,
            "status" -> "started".toJson,
            "node" -> node.toJson)
        case Progress(id, progress) =>
          JsObject(
            "id" -> id.toJson,
            "status" -> "progress".toJson,
            "progress" -> progress.toJson)
        case Done(id, result) =>
          JsObject(
            "id" -> id.toJson,
            "status" -> "done".toJson,
            "result" -> result.toJson)
        case Failed(id, reason) =>
          JsObject(
            "id" -> id.toJson,
            "status" -> "failed".toJson,
            "reason" -> reason.toJson)
        case Aborted(id) =>
          JsObject(
            "id" -> id.toJson,
            "status" -> "aborted".toJson)
      }
  }

  implicit val taskFormat: RootJsonFormat[Task] = jsonFormat2(Task)
}

sealed case class Task(service: String, parameters: Vector[String])

object JobLifecycle {
  def canAdvanceTo(oldStatus: JobStatus, newStatus: JobStatus): Boolean = {
    if (oldStatus.id != newStatus.id)
      false
    else
      (oldStatus, newStatus) match {
        case (Pending(_, _, _), Assigned(_, _) | Aborted(_)) => true
        case (Assigned(_, _), Started(_, _) | Aborted(_)) => true
        case (Started(_, _), Progress(_, _) | Done(_, _) | Failed(_, _)) => true
        case (Progress(_, _), Progress(_, _) | Done(_, _) | Failed(_, _)) => true
        case _ => false
      }
  }
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
    encode: M => Array[Byte],
    decode: Array[Byte] => M,
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
                  val m = decode(message.message)
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
        val message = encode(m)
        val keyedMessage = new kafka.producer.KeyedMessage[String, Array[Byte]](queueName, null, message)
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
      r match {
        case Submit(id, service, parameters) =>
          if (issuedIds(id)) {
            val (req, stat) = registry(service)
            req.post(r)
            val status = Pending(id, service, parameters)
            state += (id -> status)
            issuedIds -= id
            statuses.post(status)
          }
        case Report(newStatus) =>
          for (oldStatus <- state.get(newStatus.id) if JobLifecycle.canAdvanceTo(oldStatus, newStatus)) {
            state += (newStatus.id -> newStatus)
            statuses.post(newStatus)
          }
      }
    }

  def check(jobId: String): JobStatus = state(jobId)
}

trait Worker {
  def submit(id: String, task: Task): Unit
}

final class SimplifyWorker(statuses: Bus[JobStatus]) extends Worker {
  def submit(id: String, task: Task): Unit = {
    try {
      statuses.post(Assigned(id, "node"))
      statuses.post(Started(id, "node"))
      Thread.sleep(5000) // Pretend to do a lot of work
      statuses.post(Progress(id, .5))
      Thread.sleep(5000) // Continue work
      statuses.post(Done(id, "hi"))
    } catch {
      case scala.util.control.NonFatal(ex) =>
        val message: JobStatus = Failed(id = id, reason = ex.getMessage)
        statuses.post(message)
    }
  }
}
