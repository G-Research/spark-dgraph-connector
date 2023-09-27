package uk.co.gresearch

import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType

package object spark {
  // unifying access to RowEncoder.apply() and RowEncoder.encoderFor()
  implicit class ExtendedStructType(schema: StructType) {
    def encoder: Encoder[Row] = RowEncoder(schema)
  }
}
