package uk.co.gresearch.spark.dgraph.connector.model

import java.time.Clock

import com.google.gson.JsonArray
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.encoder.JsonNodeInternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.{ExecutorProvider, JsonGraphQlExecutor}
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel.filter
import uk.co.gresearch.spark.dgraph.connector.{Chunk, Logging, Partition, Uid}

import scala.collection.JavaConverters._

/**
 * Represents a specific model of graph data in a tabular form.
 */
trait GraphTableModel extends Logging {

  val execution: ExecutorProvider
  val encoder: JsonNodeInternalRowEncoder
  val chunkSize: Int

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

    log.info(s"read chunk " +
      Option(TaskContext.get()).map(tc =>
        s"in stage ${loggingFormat.format(tc.stageId())} partition ${loggingFormat.format(tc.partitionId())} "
      ).getOrElse("") +
      s"of ${loggingFormat.format(json.string.length)} bytes " +
      s"with ${loggingFormat.format(partition.predicates.size)} predicates " +
      s"for ${loggingFormat.format(chunk.length)} uids " +
      s"after ${chunk.after.toHexString} " +
      s"${until.map(e => s"until ${e.toHexString} ").getOrElse("")}" +
      s"with ${loggingFormat.format(array.size())} nodes " +
      s"in ${loggingFormat.format((endTs - startTs)/1000.0)}s")

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
