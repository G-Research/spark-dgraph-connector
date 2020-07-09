package uk.co.gresearch.spark.dgraph.connector.model

import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.{Chunk, GraphQl, PartitionQuery}

/**
 * Models only the edges of a graph as a table.
 */
case class EdgeTableModel(execution: ExecutorProvider, encoder: TripleEncoder, chunkSize: Int) extends GraphTableModel {

  /**
   * Turn a partition query into a GraphQl query.
   *
   * @param query partition query
   * @param chunk chunk of the result set to query
   * @return graphql query
   */
  override def toGraphQl(query: PartitionQuery, chunk: Option[Chunk]): GraphQl =
  // TODO: query for edges-only when supported
    query.forPropertiesAndEdges(chunk)

}
