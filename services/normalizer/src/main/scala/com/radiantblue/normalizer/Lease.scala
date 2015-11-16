package com.radiantblue.normalizer

import com.radiantblue.deployer.Deployer
import com.radiantblue.piazza.Messages._

object Lease {
  val bolt: backtype.storm.topology.IRichBolt = new backtype.storm.topology.base.BaseRichBolt {
    val logger = org.slf4j.LoggerFactory.getLogger(Lease.getClass)
    var _collector: backtype.storm.task.OutputCollector = _

    def deployer = Deployer.deployer(???)(???, ???)

    def execute(tuple: backtype.storm.tuple.Tuple): Unit = {
      val request = tuple.getValue(0).asInstanceOf[RequestLease]
      logger.info("Lease request: {}", request)
      val message = (LeaseGranted.newBuilder
        .setLocator(request.getLocator)
        .setTimeout(0)
        .setTag(request.getTag)
        .build)
      _collector.emit(tuple, java.util.Arrays.asList[AnyRef](message.toByteArray))
      logger.info("Emitted {}", message)
      _collector.ack(tuple)
    }

    def prepare(conf: java.util.Map[_, _], context: backtype.storm.task.TopologyContext, collector: backtype.storm.task.OutputCollector): Unit = {
      _collector = collector
    }

    def declareOutputFields(declarer: backtype.storm.topology.OutputFieldsDeclarer): Unit = {
      declarer.declare(new backtype.storm.tuple.Fields("message"))
    }
  }
}
