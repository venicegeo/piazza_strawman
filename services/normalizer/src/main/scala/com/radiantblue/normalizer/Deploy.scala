package com.radiantblue.normalizer

import com.radiantblue.deployer.Deployer
import com.radiantblue.piazza._
import com.radiantblue.piazza.Messages._
import com.radiantblue.piazza.postgres._

object Deploy {
  val bolt: backtype.storm.topology.IRichBolt = new backtype.storm.topology.base.BaseRichBolt {
    val logger = org.slf4j.LoggerFactory.getLogger(Deploy.getClass)
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val postgres = Postgres("piazza.metadata.postgres").connect()
      implicit val system = akka.actor.ActorSystem("leasing")
      import system.dispatcher
      import scala.concurrent.Await, scala.concurrent.duration.Duration

      try {
        val deployer = Deployer.deployer(postgres)
        val request = tuple.getValue(0).asInstanceOf[RequestDeploy]
        logger.info("Deploy request: {}", request)
        val (metadata, geometadata) = deployer.metadataStore.lookup(request.getLocator)
        val resource = deployer.dataStore.lookup(request.getLocator)
        deployer.publish.publish(metadata, geometadata, resource, request.getServer)
        deployer.track.deploymentSucceeded(request.getId)
        val deployments = postgres.getLeasesByDeployment(request.getId)
        logger.info("Reporting success for deployments: {}", deployments)
        for (lease <- deployments) {
          val message = LeaseGranted.newBuilder()
            .setLocator(request.getLocator)
            .setTimeout(0)
            .setTag(com.google.protobuf.ByteString.copyFrom(lease.tag))
            .build()
          logger.info("After successful deploy {}", message)
          _collector.emit(tuple, java.util.Arrays.asList[AnyRef](message.toByteArray))
        }
        _collector.ack(tuple)
      } catch {
        case scala.util.control.NonFatal(ex) =>
          logger.error("Error deploying data: ", ex)
          _collector.fail(tuple)
      } finally postgres.close()
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declare(new backtype.storm.tuple.Fields("message"))
    }
  }
}
