package com.radiantblue.normalizer

import kafka.consumer.Whitelist
import kafka.producer.KeyedMessage
import com.radiantblue.deployer.Deployer
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._
import com.radiantblue.piazza.postgres._

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
    val producer = com.radiantblue.piazza.kafka.Kafka.producer[String, Array[Byte]]()
    val consumer = com.radiantblue.piazza.kafka.Kafka.consumer("deploy")
    val streams = consumer.createMessageStreamsByFilter(Whitelist("deploy-requests"))
    val postgres = Postgres("piazza.metadata.postgres").connect()
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
  }
}
