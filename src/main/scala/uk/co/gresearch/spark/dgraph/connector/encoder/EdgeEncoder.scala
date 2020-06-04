package uk.co.gresearch.spark.dgraph.connector.encoder

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.{Edge, Triple, TriplesFactory, Uid}

/**
 * Encodes only triples that represent edges, i.e. object is a uid.
 */
class EdgeEncoder extends TripleEncoder {

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  override def schema(): StructType = Encoders.product[Edge].schema

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   * From: org.apache.spark.sql.connector.read.Scan.readSchema
   */
  override def readSchema(): StructType = schema()

  /**
   * Encodes a triple as an InternalRow.
   *
   * @param triple a Triple
   * @return an InternalRow
   */
  override def asInternalRow(triple: Triple): InternalRow = triple match {
    case Triple(s: Uid, p: String, o: Uid) => InternalRow(s.uid, UTF8String.fromString(p), o.uid)
    case _ => throw new IllegalArgumentException(s"Edge triple expected with object being a uid: " +
      s"Triple(" +
      s"${triple.s}: ${TriplesFactory.getType(triple.s)}, " +
      s"${triple.p}: ${TriplesFactory.getType(triple.p)}, " +
      s"${triple.o}: ${TriplesFactory.getType(triple.o)}" +
      s")"
    )
  }

}
