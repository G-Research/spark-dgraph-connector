package uk.co.gresearch.spark.dgraph.connector.model

import uk.co.gresearch.spark.dgraph.connector.encoder.JsonNodeInternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.{NoPartitionMetrics, PartitionMetrics}

/**
 * Models all triples of a graph as a table, nodes with properties and edges.
 */
case class TripleTableModel(execution: ExecutorProvider,
                            encoder: JsonNodeInternalRowEncoder,
                            chunkSize: Int,
                            metrics: PartitionMetrics = NoPartitionMetrics())
  extends GraphTableModel {

  override def withMetrics(metrics: PartitionMetrics): TripleTableModel = copy(metrics = metrics)

}
