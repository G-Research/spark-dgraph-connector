package uk.co.gresearch.spark.dgraph.connector.partitioner

import uk.co.gresearch.spark.dgraph.connector.{Partition, Target}

class SingletonPartitioner(targets: Seq[Target]) extends Partitioner {

  override def getPartitions: Seq[Partition] = Seq(Partition(targets, None))

}
