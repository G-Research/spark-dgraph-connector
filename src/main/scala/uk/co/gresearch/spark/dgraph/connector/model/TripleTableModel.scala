package uk.co.gresearch.spark.dgraph.connector.model
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.PartitionQuery
import uk.co.gresearch.spark.dgraph.connector.encoder.InternalRowEncoder

/**
 * Models all triples of a graph as a table, nodes with properties and edges.
 */
case class TripleTableModel(encoder: InternalRowEncoder) extends GraphTableModel {

  /**
   * Turn a partition query into a GraphQl query.
   *
   * @param query partition query
   * @return graphql query
   */
  override def toGraphQl(query: PartitionQuery): connector.GraphQl =
    query.forPropertiesAndEdges

}
