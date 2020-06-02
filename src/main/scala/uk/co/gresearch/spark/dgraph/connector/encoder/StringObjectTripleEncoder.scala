package uk.co.gresearch.spark.dgraph.connector.encoder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.{Schema, Triple, TriplesFactory}

/**
 * Encodes Triple by representing objects as strings.
 **/
class StringObjectTripleEncoder extends TripleEncoder {

  override def schema(): StructType = StructType(
    Array(
      StructField("subject", LongType),          // subject
      StructField("predicate", StringType),      // predicate
      StructField("object-string", StringType),  // object as a string
      StructField("object-type", StringType),    // object type
    )
  )

  override def readSchema(): StructType = schema()

  override def asInternalRow(triple: Triple): InternalRow =
    InternalRow(
      triple.s.uid,
      UTF8String.fromString(triple.p),
      UTF8String.fromString(triple.o.toString),
      UTF8String.fromString(TriplesFactory.getType(triple.o)),
    )

}
