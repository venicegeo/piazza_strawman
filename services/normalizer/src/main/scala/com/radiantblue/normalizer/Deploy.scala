package com.radiantblue.normalizer

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import com.radiantblue.deployer.Deployer
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._
import com.radiantblue.piazza.postgres._

import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object Deploy {
  val logger = org.slf4j.LoggerFactory.getLogger(Deploy.getClass)

  def thread(f: => Any): java.lang.Thread =
    new java.lang.Thread {
      override def run(): Unit = { f }
      start()
    }

  val formatLeaseGranted = toJsonBytes[LeaseGranted]
  val parseRequestDeploy = fromJsonBytes[RequestDeploy]

  def main(args: Array[String]): Unit = {
    val producer = com.radiantblue.piazza.kafka.Kafka.newProducer[String, Array[Byte]]()
    val consumer = com.radiantblue.piazza.kafka.Kafka.newConsumer[String, Array[Byte]]("deploy")
    consumer.subscribe(java.util.Arrays.asList("deploy-requests"))
    /*val streams = consumer.createMessageStreamsByFilter(Whitelist("deploy-requests"))*/
    val postgres = Postgres("piazza.metadata.postgres").connect()

    while (true) {
      val records = consumer.poll(1000)
      for(record <- records) {
        val message = record.value()
        try {
          val request = parseRequestDeploy(message/*.message*/)
          Deployer.withDeployer { deployer =>
            logger.info("Deploy request: {}", request)
            val (metadata, geometadata) = deployer.metadataStore.lookup(request.locator)
            val resource = deployer.dataStore.lookup(request.locator)
            deployer.publish.publish(metadata, geometadata, resource, request.server)
            deployer.track.deploymentSucceeded(request.deployId)
            val deployments = postgres.getLeasesByDeployment(request.deployId)
            logger.info("Reporting success for deployments: {}", deployments)
            for (lease <- deployments) {
              val grant = LeaseGranted(
                locator=request.locator,
                timeout=0,
                tag=lease.tag.to[Vector])
              logger.info("After successful deploy {}", grant)
              val relayedMessage = new ProducerRecord[String, Array[Byte]]("metadata", formatLeaseGranted(grant));
              producer.send(relayedMessage)
            }
          }
        } catch {
          case scala.util.control.NonFatal(ex) =>
            logger.error("Deployment failed", ex)
        }
      }
    }

/*
    val threads = streams.map { stream =>
      thread {
        stream.foreach { message =>
          try {
            val request = parseRequestDeploy(message.message)
            Deployer.withDeployer { deployer =>
              logger.info("Deploy request: {}", request)
              val (metadata, geometadata) = deployer.metadataStore.lookup(request.locator)
              val resource = deployer.dataStore.lookup(request.locator)
              deployer.publish.publish(metadata, geometadata, resource, request.server)
              deployer.track.deploymentSucceeded(request.deployId)
              val deployments = postgres.getLeasesByDeployment(request.deployId)
              logger.info("Reporting success for deployments: {}", deployments)
              for (lease <- deployments) {
                val grant = LeaseGranted(
                  locator=request.locator,
                  timeout=0,
                  tag=lease.tag.to[Vector])
                logger.info("After successful deploy {}", grant)
                producer.send(new KeyedMessage("lease-grants", formatLeaseGranted(grant)))
              }
            }
          } catch {
            case scala.util.control.NonFatal(ex) =>
              logger.error("Deployment failed", ex)
          }
        }
      }
    }
    threads.foreach { _.join() }
*/

  }
}
