package uk.co.gresearch

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition

package object spark {
  // back porting attribute inputPartition from Spark 3.3 to make tests consistent
  implicit class ExtendedDataSourceRDDPartition(rddp: DataSourceRDDPartition) {
    def inputPartitions: Seq[InputPartition] = Seq(rddp.inputPartition)
  }
}
