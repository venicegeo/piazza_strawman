package com.radiantblue.normalizer

import com.radiantblue.deployer.Deployer
import com.radiantblue.piazza.Messages._
import com.radiantblue.piazza.postgres._

object Lease {
  val bolt: backtype.storm.topology.IRichBolt = new backtype.storm.topology.base.BaseRichBolt {
    val logger = org.slf4j.LoggerFactory.getLogger(Lease.getClass)
    var _collector: backtype.storm.task.OutputCollector = _

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val postgres = Postgres("piazza.metadata.postgres").connect()
      implicit val system = akka.actor.ActorSystem("leasing")
      import system.dispatcher
      import scala.concurrent.Await, scala.concurrent.duration.Duration

      try {
        val deployer = Deployer.deployer(postgres)
        val request = tuple.getValue(0).asInstanceOf[RequestLease]
        logger.info("Lease request: {}", request)
        val deploymentInfoF = deployer.attemptDeploy(request.getLocator, request.getTag.toByteArray)
        val (lease, status) = Await.result(deploymentInfoF, Duration.Inf)
        logger.info("tag: {}", lease.tag.map(c => f"$c%02X").mkString)
        val leases = postgres.getLeasesByDeployment(lease.deployment)
        logger.info("Leases: {}", leases)
        for (lease <- leases) {
          val message = (LeaseGranted.newBuilder
            .setLocator(request.getLocator)
            .setTimeout(0)
            .setTag(com.google.protobuf.ByteString.copyFrom(lease.tag))
            .build)
          logger.info("ltag: {}", lease.tag.map(c => f"$c%02X").mkString)
          _collector.emit(tuple, java.util.Arrays.asList[AnyRef](message.toByteArray))
          logger.info("Emitted {}", message)
        }
        _collector.ack(tuple)
      } catch {
        case scala.util.control.NonFatal(ex) => logger.error("Error granting lease: " + ex)
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
