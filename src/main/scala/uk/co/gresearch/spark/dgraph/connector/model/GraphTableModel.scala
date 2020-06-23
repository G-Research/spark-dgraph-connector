package uk.co.gresearch.spark.dgraph.connector.model

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.{GraphQl, Partition, PartitionQuery}
import uk.co.gresearch.spark.dgraph.connector.encoder.InternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.{DgraphExecutor, JsonGraphQlExecutor, ExecutorProvider}

/**
 * Represents a specific model of graph data in a tabular form.
 */
trait GraphTableModel {

  val execution: ExecutorProvider
  val encoder: InternalRowEncoder

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  def schema(): StructType = encoder.schema()

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   * From: org.apache.spark.sql.connector.read.Scan.readSchema
   */
  def readSchema(): StructType = encoder.readSchema()

  /**
   * Models the data of a partition in tabular form of InternalRows.
   *
   * @param partition a partition
   * @return graph data in tabular form
   */
  def modelPartition(partition: Partition): Iterator[InternalRow] = {
    val query = partition.query
    val graphql = toGraphQl(query)
    val executor = execution.getExecutor(partition)
    val json = executor.query(graphql)
    encoder.fromJson(json, query.resultName)
  }

  /**
   * Turn a partition query into a GraphQl query.
   * @param query partition query
   * @return graphql query
   */
  def toGraphQl(query: PartitionQuery): GraphQl

}
