package uk.co.gresearch.spark.dgraph.connector

import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import org.apache.spark.sql.connector.read.InputPartition

/**
 * Partition of Dgraph data. Reads all triples with the given predicates.
 *
 * @param targets Dgraph alpha nodes
 * @param predicates optional predicates to read
 */
case class Partition(targets: Seq[Target], predicates: Option[Set[Predicate]], uids: Option[UidRange]) extends InputPartition {

  // TODO: use host names of Dgraph alphas to co-locate partitions
  override def preferredLocations(): Array[String] = super.preferredLocations()

  /**
   * Reads the entire partition and returns all triples.
   * @return triples
   */
  def getTriples: Iterator[Triple] = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val query = Query.forAllPropertiesAndEdges("data", predicates, uids)
      val client: DgraphClient = getClientFromChannel(channels)
      val response: Response = client.newReadOnlyTransaction().query(query)
      val json: String = response.getJson.toStringUtf8
      TriplesFactory.fromJson(json, "data", predicates)
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
