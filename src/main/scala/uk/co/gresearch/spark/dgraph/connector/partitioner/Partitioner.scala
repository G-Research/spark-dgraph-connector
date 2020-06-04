package uk.co.gresearch.spark.dgraph.connector.partitioner

import uk.co.gresearch.spark.dgraph.connector.Partition

trait Partitioner {

  def getPartitions: Seq[Partition]

}
