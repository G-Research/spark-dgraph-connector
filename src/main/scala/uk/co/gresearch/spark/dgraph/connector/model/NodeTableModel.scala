package uk.co.gresearch.spark.dgraph.connector.model
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.PartitionQuery
import uk.co.gresearch.spark.dgraph.connector.encoder.InternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider

/**
 * Models only the nodes of a graph as a table.
 */
case class NodeTableModel(execution: ExecutorProvider, encoder: InternalRowEncoder) extends GraphTableModel {

  /**
   * Turn a partition query into a GraphQl query.
   *
   * @param query partition query
   * @return graphql query
   */
  override def toGraphQl(query: PartitionQuery): connector.GraphQl =
    query.forProperties

}
