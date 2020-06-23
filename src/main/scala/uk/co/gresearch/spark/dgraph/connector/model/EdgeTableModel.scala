package uk.co.gresearch.spark.dgraph.connector.model
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.PartitionQuery
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider

/**
 * Models only the edges of a graph as a table.
 */
case class EdgeTableModel(execution: ExecutorProvider, encoder: TripleEncoder) extends GraphTableModel {

  /**
   * Turn a partition query into a GraphQl query.
   *
   * @param query partition query
   * @return graphql query
   */
  override def toGraphQl(query: PartitionQuery): connector.GraphQl =
  // TODO: query for edges-only when supported
    query.forPropertiesAndEdges

}
