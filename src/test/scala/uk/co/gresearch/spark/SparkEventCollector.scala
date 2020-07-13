package uk.co.gresearch.spark

import org.apache.spark.scheduler.{AccumulableInfo, SparkListener, SparkListenerStageCompleted}

import scala.collection.mutable

case class SparkEventCollector(accus: mutable.MutableList[AccumulableInfo]) extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
    accus ++= stageCompleted.stageInfo.accumulables.values
}
