package uk.co.gresearch.spark.dgraph.connector.model

import uk.co.gresearch.spark.dgraph.connector.{NoPartitionMetrics, PartitionMetrics}
import uk.co.gresearch.spark.dgraph.connector.encoder.JsonNodeInternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider

case class TestModel(execution: ExecutorProvider,
                     encoder: JsonNodeInternalRowEncoder,
                     chunkSize: Int,
                     metrics: PartitionMetrics = NoPartitionMetrics())
  extends GraphTableModel {
  override def withMetrics(metrics: PartitionMetrics): TestModel = copy(metrics = metrics)
}
