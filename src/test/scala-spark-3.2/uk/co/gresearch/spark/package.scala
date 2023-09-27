package uk.co.gresearch

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.types.StructType

package object spark {
  // back porting attribute inputPartition from Spark 3.3 to make tests consistent
  implicit class ExtendedDataSourceRDDPartition(rddp: DataSourceRDDPartition) {
    def inputPartitions: Seq[InputPartition] = Seq(rddp.inputPartition)
  }
  // unifying access to RowEncoder.apply() and RowEncoder.encoderFor()
  implicit class ExtendedStructType(schema: StructType) {
    def encoder: Encoder[Row] = RowEncoder(schema)
  }
}
