package uk.co.gresearch.spark.dgraph.connector.model

import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.{NoPartitionMetrics, PartitionMetrics}

/**
 * Models only the edges of a graph as a table.
 */
case class EdgeTableModel(execution: ExecutorProvider,
                          encoder: TripleEncoder,
                          chunkSize: Int,
                          metrics: PartitionMetrics = NoPartitionMetrics())
  extends GraphTableModel {

  override def withMetrics(metrics: PartitionMetrics): EdgeTableModel = copy(metrics = metrics)

}
