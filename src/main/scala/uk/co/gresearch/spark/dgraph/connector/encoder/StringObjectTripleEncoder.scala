package uk.co.gresearch.spark.dgraph.connector.encoder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.Triple

/**
 * Encodes Triple by representing objects as strings.
 **/
class StringObjectTripleEncoder extends TripleEncoder {

  override def schema(): StructType = StructType(
    Array(
      StructField("s", LongType),    // subject
      StructField("p", StringType),  // predicate
      StructField("o", StringType)   // object as a string
    )
  )

  override def readSchema(): StructType = schema()

  override def asInternalRow(triple: Triple): InternalRow =
    InternalRow(
      triple.s,
      UTF8String.fromString(triple.p),
      UTF8String.fromString(triple.o)
    )

}
