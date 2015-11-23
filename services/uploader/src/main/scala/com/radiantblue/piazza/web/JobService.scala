package com.radiantblue.piazza.web

import akka.actor.Actor
import scala.concurrent.Future
import spray.routing._
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json._

class JobServiceActor extends Actor with JobService {
  def actorRefFactory = context
  def executionContext = context.dispatcher
  val requests = Bus.of[JobRequest]
  val statuses = Bus.of[JobStatus]
  val simplify = {
    val request = Bus.of[JobRequest]
    val report = Bus.of[JobStatus]
    val worker = new SimplifyWorker(report)
    request.subscribe { case Submit(i, t) => worker.submit(i, t) }
    (request, report)
  }
  val registry = Map("simplify" -> simplify)
  val manager = new MemoryManager(requests, statuses, registry)
  statuses.subscribe(println(_))
  simplify._2.subscribe(status => requests.post(Report(status)))
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
          complete { 
            Future {
              val jobId = manager.nextId
              requests.post(Submit(jobId, task))
              HttpResponse(
                status = StatusCodes.Found,
                headers = List(HttpHeaders.Location(s"/jobs/$jobId")))
            }
          }
        }
      }
    }
}

trait JobJsonProtocol extends DefaultJsonProtocol {
  implicit val statusFormat: RootJsonFormat[JobStatus] = new RootJsonFormat[JobStatus] {
    def read(json: JsValue): JobStatus =
      json.asJsObject.getFields("status", "id") match {
        case Seq(JsString("pending"), JsString(id)) =>
          val Seq(task) = json.asJsObject.getFields("task")
          Pending(id, task.convertTo[Task])
        case Seq(JsString("assigned"), JsString(id)) => Assigned(id)
        case Seq(JsString("started"), JsString(id)) => Started(id)
        case Seq(JsString("progress"), JsString(id)) => Progress(id)
        case Seq(JsString("done"), JsString(id)) => Done(id)
        case Seq(JsString("failed"), JsString(id)) => Failed(id)
        case Seq(JsString("aborted"), JsString(id)) => Aborted(id)
        case _ => throw new DeserializationException("not a valid status")
      }

    def write(status: JobStatus): JsValue = 
      status match {
        case Pending(id, task) =>
          JsObject("id" -> JsString(id), "status" -> JsString("pending"), "task" -> task.toJson)
        case Assigned(id) => 
          JsObject("id" -> JsString(id), "status" -> JsString("assigned"))
        case Started(id) => 
          JsObject("id" -> JsString(id), "status" -> JsString("started"))
        case Progress(id) => 
          JsObject("id" -> JsString(id), "status" -> JsString("progress"))
        case Done(id) => 
          JsObject("id" -> JsString(id), "status" -> JsString("done"))
        case Failed(id) => 
          JsObject("id" -> JsString(id), "status" -> JsString("failed"))
        case Aborted(id) => 
          JsObject("id" -> JsString(id), "status" -> JsString("aborted"))
      }
  }

  implicit val taskFormat: RootJsonFormat[Task] = jsonFormat2(Task)
}

sealed case class Task(service: String, parameters: Vector[String])

sealed trait JobStatus {
  def id: String
  def canAdvanceTo(status: JobStatus): Boolean = false
}

final case class Pending(id: String, task: Task) extends JobStatus {
  override def canAdvanceTo(status: JobStatus) =
    status match {
      case Assigned(`id`)
         | Aborted(`id`) => true
      case _ => false
    }
}

final case class Assigned(id: String) extends JobStatus {
  override def canAdvanceTo(status: JobStatus) = 
    status match {
      case Started(`id`)
         | Aborted(`id`) => true
      case _ => false
    }
}

final case class Started(id: String) extends JobStatus {
  override def canAdvanceTo(status: JobStatus) = 
    status match {
      case Progress(`id`)
         | Done(`id`)
         | Failed(`id`) => true
      case _ => false
    }
}

final case class Progress(id: String) extends JobStatus {
  override def canAdvanceTo(status: JobStatus) = 
    status match {
      case Done(`id`)
         | Failed(`id`) => true
      case _ => false
    }
}

final case class Done(id: String) extends JobStatus
final case class Failed(id: String) extends JobStatus
final case class Aborted(id: String) extends JobStatus

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
}

trait Manager {
  // def request(r: JobRequest): Unit
  def check(jobId: String): JobStatus
  def nextId(): String
}

final class MemoryManager(requests: Bus[JobRequest], statuses: Bus[JobStatus], registry: Map[String, (Bus[JobRequest], Bus[JobStatus])]) extends Manager {
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
        case Submit(id, task) =>
          if (issuedIds(id)) {
            val (req, stat) = registry(task.service)
            req.post(Submit(id, task))
            val status = Pending(id, task)
            state += (id -> status)
            issuedIds -= id
            statuses.post(status)
          }
        case Report(newStatus) =>
          for (oldStatus <- state.get(newStatus.id) if oldStatus.canAdvanceTo(newStatus)) {
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
      println("Worker")
      statuses.post(Assigned(id))
      statuses.post(Started(id))
      Thread.sleep(5000) // Pretend to do a lot of work
      statuses.post(Progress(id))
      Thread.sleep(5000) // Continue work
      statuses.post(Done(id))
    } catch {
      case scala.util.control.NonFatal(_) => statuses.post(Failed(id))
    }
  }
}

sealed trait JobRequest
final case class Submit(id: String, task: Task) extends JobRequest
final case class Report(status: JobStatus) extends JobRequest
