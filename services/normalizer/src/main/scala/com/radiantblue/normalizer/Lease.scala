package com.radiantblue.normalizer

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import com.radiantblue.deployer.Deployer
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._
import com.radiantblue.piazza.postgres._

object Lease {
  val logger = org.slf4j.LoggerFactory.getLogger(Lease.getClass)

  def thread(f: => Any): java.lang.Thread =
    new java.lang.Thread {
      override def run(): Unit = { f }
      start()
    }

  val formatLeaseGranted = toJsonBytes[LeaseGranted]
  val formatRequestDeploy = toJsonBytes[RequestDeploy]
  val parseRequestLease = fromJsonBytes[RequestLease]

  def main(args: Array[String]): Unit = {
    val producer = com.radiantblue.piazza.kafka.Kafka.producer[String, Array[Byte]]()
    val consumer = com.radiantblue.piazza.kafka.Kafka.consumer("lease")
    val streams = consumer.createMessageStreamsByFilter(Whitelist("lease-requests"))
    val threads = streams.map { stream =>
      thread {
        stream.foreach { message =>
          try {
            Deployer.withDeployer { deployer =>
              val request = parseRequestLease(message.message)
              logger.info("Lease request: {}", request)
              val status = deployer.track.deploymentStatus(request.locator)
              status match {
                case Starting(id) =>
                  deployer.leasing.attachLease(request.locator, id, request.tag.to[Array])
                  // no message sent at this point, grant will be sent on completion
                case Live(id, server) =>
                  val format = toJsonBytes[LeaseGranted]
                  deployer.leasing.attachLease(request.locator, id, request.tag.to[Array])
                  // send message to lease-grants that lease is granted
                  val message = LeaseGranted(
                    locator=request.locator,
                    timeout=request.timeout,
                    tag=request.tag)
                  producer.send(new KeyedMessage("lease-grants", formatLeaseGranted(message)))
                case Dead =>
                  val (server, id) = deployer.track.deploymentStarted(request.locator)
                  deployer.leasing.attachLease(request.locator, id, request.tag.to[Array])
                  val message = RequestDeploy(
                    locator=request.locator,
                    server=server,
                    deployId=id)
                  producer.send(new KeyedMessage("deploy-requests", formatRequestDeploy(message)))
              }
            }
          } catch {
            case scala.util.control.NonFatal(ex) => logger.error("Error in lease manager", ex)
          }
        }
      }
    }
    threads.foreach { _.join() }
  }
}
