package uk.co.gresearch.spark.dgraph.connector.model

import java.time.Clock

import com.google.gson.JsonArray
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.encoder.{InternalRowEncoder, JsonNodeInternalRowEncoder}
import uk.co.gresearch.spark.dgraph.connector.executor.{ExecutorProvider, JsonGraphQlExecutor}
import uk.co.gresearch.spark.dgraph.connector.{Chunk, GraphQl, Partition, PartitionQuery}

/**
 * Represents a specific model of graph data in a tabular form.
 */
trait GraphTableModel {

  val execution: ExecutorProvider
  val encoder: InternalRowEncoder
  val chunkSize: Option[Int]

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
  def modelPartition(partition: Partition): Iterator[InternalRow] = chunkSize match {
    case Some(size) => readChunks(partition, size)
    case None => readSingleChunk(partition)
  }

  def readSingleChunk(partition: Partition): Iterator[InternalRow] = {
    val query = partition.query
    val graphql = toGraphQl(query, None)
    val executor = execution.getExecutor(partition)
    val json = executor.query(graphql)
    encoder.fromJson(json, query.resultName)
  }

  def readChunks(partition: Partition, size: Int): Iterator[InternalRow] = {
    encoder match {
      case e: JsonNodeInternalRowEncoder =>
        readChunks(partition, execution.getExecutor(partition), e, size)
      case _ => throw new IllegalArgumentException(s"Partitions can only be read in chunks " +
        s"with a ${classOf[JsonNodeInternalRowEncoder].getSimpleName}, not ${encoder.getClass.getSimpleName}")
    }
  }

  def readChunks(partition: Partition,
                 executor: JsonGraphQlExecutor,
                 encoder: JsonNodeInternalRowEncoder,
                 size: Int): Iterator[InternalRow] =
    ChunkIterator(size, readChunk(partition, executor, encoder))
      .flatMap(encoder.fromJson)

  def readChunk(partition: Partition,
                executor: JsonGraphQlExecutor,
                encoder: JsonNodeInternalRowEncoder)
               (chunk: Chunk): JsonArray = {
    val query = partition.query
    val start = Clock.systemUTC().instant().toEpochMilli
    val graphql = toGraphQl(query, Some(chunk))
    val json = executor.query(graphql)
    val end = Clock.systemUTC().instant().toEpochMilli
    val array = encoder.getResult(json, query.resultName)
    println(s"stage=${Option(TaskContext.get()).map(_.stageId()).orNull} part=${Option(TaskContext.get()).map(_.partitionId()).orNull}: " +
      s"read ${json.string.length} bytes with ${partition.predicates.map(p => s"${p.size} predicates for ").get}${chunk.length} " +
      s"uids from ${chunk.after.toHexString} with ${array.size()} nodes in ${(end - start)/1000.0}s")
    array
  }

  /**
   * Turn a partition query into a GraphQl query.
   * @param query partition query
   * @param chunk chunk of the result set to query
   * @return graphql query
   */
  def toGraphQl(query: PartitionQuery, chunk: Option[Chunk]): GraphQl

}
