package uk.co.gresearch.spark.dgraph.connector.model

import uk.co.gresearch.spark.dgraph.connector.encoder.JsonNodeInternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider

/**
 * Models only the nodes of a graph as a table.
 */
case class NodeTableModel(execution: ExecutorProvider,
                          encoder: JsonNodeInternalRowEncoder,
                          chunkSize: Int)
  extends GraphTableModel {

  override def withEncoder(encoder: JsonNodeInternalRowEncoder): GraphTableModel = copy(encoder = encoder)

}
