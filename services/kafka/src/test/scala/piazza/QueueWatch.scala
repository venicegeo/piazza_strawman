package piazza

import com.radiantblue.piazza.kafka.Kafka
import com.radiantblue.piazza.Messages._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration

object QueueWatch {
  import scala.concurrent.ExecutionContext.Implicits.global

  val parsers = Vector[Array[Byte] => com.google.protobuf.GeneratedMessage](
    Upload.parseFrom _,
    Metadata.parseFrom _,
    GeoMetadata.parseFrom _,
    RequestLease.parseFrom _,
    LeaseGranted.parseFrom _,
    Simplify.parseFrom _
  )

  def parser(fs: (Array[Byte] => com.google.protobuf.GeneratedMessage)*): Array[Byte] => Option[com.google.protobuf.GeneratedMessage] = {
    bytes => {
      fs.map(f => scala.util.Try(f(bytes)).toOption)
        .collectFirst { case Some(result) => result }
    }
  }

  val findParserForTopic: String => (Array[Byte] => Option[com.google.protobuf.GeneratedMessage]) = {
    case "metadata" => parser(GeoMetadata.parseFrom _, Metadata.parseFrom _)
    case "uploads" => parser(Upload.parseFrom _)
    case "lease-requests" => parser(RequestLease.parseFrom _)
    case "lease-grants" => parser(LeaseGranted.parseFrom _)
    case "simplify-requests" => parser(Simplify.parseFrom _)
    case _ => Function.const(None)
  }

  def first[A,B](a: Future[A], b: Future[B]): Future[Either[A,B]] =
    Future.firstCompletedOf(Iterable(a map (Left(_)), b map (Right(_))))

  def format(message: kafka.message.MessageAndMetadata[Array[Byte], Array[Byte]]): String = {
    val body = 
      findParserForTopic(message.topic)(message.message) match {
        case Some(m) => m.toString
        case None => message.message.map(c => f"$c%02X").mkString
      }
    s"${message.topic}:\n$body"
  }

  def main(args: Array[String]): Unit = {
    val consumer = Kafka.consumer("queue-watch")
    val List(stream) = consumer.createMessageStreamsByFilter(kafka.consumer.Whitelist(".*"))
    val readStream = Future {
      stream.foreach { message =>
        println(format(message))
      }
    }
    val enter = Future { Console.readLine }
    Await.ready(first(enter, readStream), Duration.Inf)
    consumer.shutdown()
  }
}

object QueuePost {
  def main(args: Array[String]): Unit = {
    val producer = Kafka.producer[String, Array[Byte]]()
    val message = Simplify.newBuilder
      .setLocator(args.head)
      .setTolerance(0.1)
      .build()
    val keyedMessage = 
      new kafka.producer.KeyedMessage[String, Array[Byte]]("simplify-requests", message.toByteArray)
    producer.send(keyedMessage)
    println(s"Sent $message")
  }
}
