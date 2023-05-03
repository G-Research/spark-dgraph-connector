package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import uk.co.gresearch.spark.dgraph.connector.{Partition, Uid}

trait UidDetector {
  /**
   * Query the partition for consecutive uids starting at the given uid.
   *
   * @param partition partition to query
   * @param uid       first uids to retrieve
   * @param uids      number of uids to retrieve
   * @return sequence of uids after given uid (including)
   */
  def getUids(partition: Partition, uid: Uid, uids: Int): Seq[Uid]
}
