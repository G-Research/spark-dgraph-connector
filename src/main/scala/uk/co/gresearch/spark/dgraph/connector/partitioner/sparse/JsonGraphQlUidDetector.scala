package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.encoder.JsonGraphQlResultParser
import uk.co.gresearch.spark.dgraph.connector.executor.JsonGraphQlExecutor
import uk.co.gresearch.spark.dgraph.connector.{Chunk, Logging, Partition, Uid}

import java.time.Clock

case class JsonGraphQlUidDetector(executor: JsonGraphQlExecutor)
    extends UidDetector
    with JsonGraphQlResultParser
    with Logging {

  /**
   * Query the partition for consecutive uids starting at the given uid.
   *
   * @param partition
   *   partition to query
   * @param uid
   *   first uids to retrieve, must be larger than 0x0
   * @param uids
   *   number of uids to retrieve
   * @return
   *   sequence of uids after given uid (including)
   */
  override def getUids(partition: Partition, uid: connector.Uid, uids: Int): Seq[connector.Uid] = {
    val query = partition.getNone.query
    val startTs = Clock.systemUTC().instant().toEpochMilli
    val chunk = Chunk(uid.before, uids)
    val graphql = query.forChunk(Some(chunk))
    val json = executor.query(graphql)
    val endTs = Clock.systemUTC().instant().toEpochMilli

    val uidRegion = getNodes(getResult(json, query.resultName))
      .map(_.get("uid").getAsString)
      .map(Uid.apply)
      .toSeq

    log.info(
      s"read uid chunk " +
        s"of ${loggingFormat.format(json.string.length)} bytes " +
        s"with ${loggingFormat.format(partition.predicates.size)} predicates " +
        s"for ${loggingFormat.format(uids)} uids " +
        s"at ${uid.toHexString} " +
        s"with ${loggingFormat.format(uids)} uids " +
        s"in ${loggingFormat.format((endTs - startTs) / 1000.0)}s"
    )

    uidRegion
  }
}
