package uk.co.gresearch.spark.dgraph.connector.encoder

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.{DGraphStringObjectRow, Triple, TriplesFactory}

/**
 * Encodes Triple by representing objects as strings.
 **/
class StringObjectTripleEncoder extends TripleEncoder {

  override def schema(): StructType = Encoders.product[DGraphStringObjectRow].schema

  override def readSchema(): StructType = schema()

  override def asInternalRow(triple: Triple): InternalRow =
    InternalRow(
      triple.s.uid,
      UTF8String.fromString(triple.p),
      UTF8String.fromString(triple.o.toString),
      UTF8String.fromString(TriplesFactory.getType(triple.o)),
    )

}
