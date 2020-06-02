package uk.co.gresearch.spark.dgraph.connector.encoder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.Triple

trait TripleEncoder extends Serializable {
  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  def schema(): StructType

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   * From: org.apache.spark.sql.connector.read.Scan.readSchema
   */
  def readSchema(): StructType

  /**
   * Encodes a triple as an InternalRow.
    * @param triple a Triple
   * @return an InternalRow
   */
  def asInternalRow(triple: Triple): InternalRow
}
