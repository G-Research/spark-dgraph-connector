package uk.co.gresearch.spark.dgraph.connector.model

import uk.co.gresearch.spark.dgraph.connector.encoder.JsonNodeInternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.{Chunk, GraphQl, NoPartitionMetrics, PartitionMetrics, PartitionQuery}

/**
 * Models only the nodes of a graph as a table.
 */
case class NodeTableModel(execution: ExecutorProvider,
                          encoder: JsonNodeInternalRowEncoder,
                          chunkSize: Int,
                          metrics: PartitionMetrics = NoPartitionMetrics())
  extends GraphTableModel {

  /**
   * Turn a partition query into a GraphQl query.
   *
   * @param query partition query
   * @param chunk chunk of the result set to query
   * @return graphql query
   */
  override def toGraphQl(query: PartitionQuery, chunk: Option[Chunk]): GraphQl =
    query.forProperties(chunk)

  override def withMetrics(metrics: PartitionMetrics): NodeTableModel = copy(metrics = metrics)

}
