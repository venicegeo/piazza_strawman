package com.radiantblue.normalizer

import com.radiantblue.deployer.Deployer
import com.radiantblue.piazza._
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
        val status = deployer.track.deploymentStatus(request.getLocator)
        status match {
          case Starting(id) =>
            deployer.leasing.attachLease(request.getLocator, id, request.getTag.toByteArray)
            // no message sent at this point, grant will be sent on completion
          case Live(id, server) =>
            deployer.leasing.attachLease(request.getLocator, id, request.getTag.toByteArray)
            // send message to lease-grants that lease is granted
            val message = (LeaseGranted.newBuilder
              .setLocator(request.getLocator)
              .setTimeout(0)
              .setTag(request.getTag)
              .build())
            _collector.emit("lease-grants", tuple, java.util.Arrays.asList[AnyRef](message.toByteArray))
          case Killing | Dead =>
            val (server, id) = deployer.track.deploymentStarted(request.getLocator)
            deployer.leasing.attachLease(request.getLocator, id, request.getTag.toByteArray)
            val message = (RequestDeploy.newBuilder
              .setLocator(request.getLocator)
              .setServer(server)
              .setId(id)
              .setTag(request.getTag)
              .build())
            _collector.emit("deploy-requests", tuple, java.util.Arrays.asList[AnyRef](message))
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
