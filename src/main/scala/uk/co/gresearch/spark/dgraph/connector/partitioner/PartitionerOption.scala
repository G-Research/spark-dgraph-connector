package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Schema, Transaction}

class PartitionerOption(partitionerOption: String) extends ConfigPartitionerOption {

  override def getPartitioner(schema: Schema,
                              clusterState: ClusterState,
                              transaction: Option[Transaction],
                              options: CaseInsensitiveStringMap): Option[Partitioner] =
    Some(getPartitioner(partitionerOption, schema, clusterState, transaction, options))

}
