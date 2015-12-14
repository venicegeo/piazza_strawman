package com.radiantblue.normalizer

import com.radiantblue.deployer.Deployer
import com.radiantblue.piazza._
import com.radiantblue.piazza.JsonProtocol._
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
        Deployer.withDeployer { deployer =>
          val request = tuple.getValue(0).asInstanceOf[RequestLease]
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
              _collector.emit("lease-grants", tuple, java.util.Arrays.asList[AnyRef](format(message)))
            case Dead =>
              val (server, id) = deployer.track.deploymentStarted(request.locator)
              deployer.leasing.attachLease(request.locator, id, request.tag.to[Array])
              val message = RequestDeploy(
                locator=request.locator,
                server=server,
                deployId=id)
              _collector.emit("deploy-requests", tuple, java.util.Arrays.asList[AnyRef](message))
          }
        }
        _collector.ack(tuple)
      } catch {
        case scala.util.control.NonFatal(ex) =>
          logger.error("Error granting lease: ", ex)
          _collector.fail(tuple)
      } finally postgres.close()
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declareStream("lease-grants", new backtype.storm.tuple.Fields("message"))
      declarer.declareStream("deploy-requests", new backtype.storm.tuple.Fields("message"))
    }
  }
}
