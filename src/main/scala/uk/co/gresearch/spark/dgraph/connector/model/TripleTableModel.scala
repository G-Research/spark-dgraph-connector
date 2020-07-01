package uk.co.gresearch.spark.dgraph.connector.model
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.{Chunk, GraphQl, PartitionQuery}
import uk.co.gresearch.spark.dgraph.connector.encoder.InternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider

/**
 * Models all triples of a graph as a table, nodes with properties and edges.
 */
case class TripleTableModel(execution: ExecutorProvider, encoder: InternalRowEncoder, chunkSize: Option[Int]) extends GraphTableModel {

  /**
   * Turn a partition query into a GraphQl query.
   *
   * @param query partition query
   * @param chunk chunk of the result set to query
   * @return graphql query
   */
  override def toGraphQl(query: PartitionQuery, chunk: Option[Chunk]): GraphQl =
    query.forPropertiesAndEdges(chunk)

}
