package uk.co.gresearch

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition

package object spark {
  // recreating attribute inputPartitions removed in Spark 4.2 to make tests consistent
  implicit class ExtendedDataSourceRDDPartition(rddp: DataSourceRDDPartition) {
    def inputPartitions: Seq[InputPartition] = rddp.inputPartition.map(Seq(_)).getOrElse(Seq.empty)
  }
}
