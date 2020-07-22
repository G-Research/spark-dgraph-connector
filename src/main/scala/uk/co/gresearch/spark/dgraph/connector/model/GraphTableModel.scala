package uk.co.gresearch.spark.dgraph.connector.model

import java.time.Clock

import com.google.gson.JsonArray
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.encoder.JsonNodeInternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.{ExecutorProvider, JsonGraphQlExecutor}
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel.filter
import uk.co.gresearch.spark.dgraph.connector.{Chunk, Partition, PartitionMetrics, Uid}

import scala.collection.JavaConverters._

/**
 * Represents a specific model of graph data in a tabular form.
 */
trait GraphTableModel {

  val execution: ExecutorProvider
  val encoder: JsonNodeInternalRowEncoder
  val chunkSize: Int
  val metrics: PartitionMetrics

  def withMetrics(metrics: PartitionMetrics): GraphTableModel

  /**
   * Sets the schema of this model. This model may only partially or not at all use the given schema.
   * @param schema a schema
   * @return model with the given schema
   */
  def withSchema(schema: StructType): GraphTableModel = withEncoder(encoder.withSchema(schema))

  /**
   * Sets the encoder of this model. Returns a copy of this model with the new encoder set.
   * @param encoder an encoder
   * @return model with the given encoder
   */
  def withEncoder(encoder: JsonNodeInternalRowEncoder): GraphTableModel = this

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
    val executor: JsonGraphQlExecutor = execution.getExecutor(partition)
    val after = partition.uidRange.map(range => range.first.before).getOrElse(Uid("0x0"))
    val until = partition.uidRange.map(range => range.until)
    ChunkIterator(after, until, chunkSize, readChunk(partition, executor, encoder, until))
      .flatMap(encoder.fromJson)
  }

  def readChunk(partition: Partition,
                executor: JsonGraphQlExecutor,
                encoder: JsonNodeInternalRowEncoder,
                until: Option[Uid])
               (chunk: Chunk): JsonArray = {
    val query = partition.query
    val startTs = Clock.systemUTC().instant().toEpochMilli
    val graphql = query.forChunk(Some(chunk))
    val json = executor.query(graphql)
    val endTs = Clock.systemUTC().instant().toEpochMilli
    val array = until.foldLeft(encoder.getResult(json, query.resultName))(filter)
    println(s"stage=${Option(TaskContext.get()).map(_.stageId()).orNull} part=${Option(TaskContext.get()).map(_.partitionId()).orNull}: " +
      s"read ${json.string.length} bytes with ${partition.predicates.size} predicates for " +
      s"${chunk.length} uids after ${chunk.after.toHexString} ${until.map(e => s"until ${e.toHexString} ").getOrElse("")}" +
      s"with ${array.size()} nodes in ${(endTs - startTs)/1000.0}s")

    metrics.incReadBytes(json.string.getBytes.length)
    metrics.incReadUids(array.size())
    metrics.incReadChunks(1)
    metrics.incChunkTime((endTs - startTs)/1000.0)

    array
  }

}

object GraphTableModel {

  def filter(array: JsonArray, until: Uid): JsonArray =
    if (array.size() == 0 || ChunkIterator.getUid(array.get(array.size()-1)) < until) {
      array
    } else {
      array.asScala
        .filter(e => ChunkIterator.getUid(e) < until)
        .foldLeft(new JsonArray()) { case (array, element) => array.add(element); array }
    }

}
