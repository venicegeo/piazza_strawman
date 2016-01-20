package com.radiantblue.normalizer

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import com.radiantblue.deployer.Deployer
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._
import com.radiantblue.piazza.postgres._

import java.io._
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

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
    val producer = com.radiantblue.piazza.kafka.Kafka.newProducer[String, Array[Byte]]()
    val consumer = com.radiantblue.piazza.kafka.Kafka.newConsumer[String, Array[Byte]]("lease")
    consumer.subscribe(java.util.Arrays.asList("lease-requests"))

    val fw = new PrintWriter(new File("leaser" ))

    while (true) {
      val records = consumer.poll(1000)
      for(record <- records) {
        val message = record.value()
        
        fw.write("Received Lease request.\n")
        fw.flush()
        try {
          Deployer.withDeployer { deployer =>
            val request = parseRequestLease(message/*.message*/)
            logger.info("Lease request: {}", request)
            val status = deployer.track.deploymentStatus(request.locator)

            fw.write("Matching status of lease.\n")
            fw.flush()

            status match {
              case Starting(id) =>
                deployer.leasing.attachLease(request.locator, id, request.tag.to[Array])
                // no message sent at this point, grant will be sent on completion
              case Live(id, server) =>
                val format = toJsonBytes[LeaseGranted]
                deployer.leasing.attachLease(request.locator, id, request.tag.to[Array])
                // send message to lease-grants that lease is granted
                val formattedLease = LeaseGranted(
                  locator=request.locator,
                  timeout=request.timeout,
                  tag=request.tag)
                val keyedMessage = new ProducerRecord[String, Array[Byte]]("lease-grants", formatLeaseGranted(formattedLease))
                producer.send(keyedMessage)
              case Dead =>
                fw.write("Dead. Starting deployment of new service.\n")
                fw.flush()
                val (server, id) = deployer.track.deploymentStarted(request.locator)
                deployer.leasing.attachLease(request.locator, id, request.tag.to[Array])
                val formattedDeploy = RequestDeploy(
                  locator=request.locator,
                  server=server,
                  deployId=id)
                val keyedMessage = new ProducerRecord[String, Array[Byte]]("deploy-requests", formatRequestDeploy(formattedDeploy))
                producer.send(keyedMessage)
            }
          }
        } catch {
          case ex: Exception => {
            fw.write("Error in leaser: " + ex + ".\n")
            fw.flush()          
          }          
        }
      }
    }

    /*val streams = consumer.createMessageStreamsByFilter(Whitelist("lease-requests"))
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
    threads.foreach { _.join() }*/
    fw.close()
  }
}
