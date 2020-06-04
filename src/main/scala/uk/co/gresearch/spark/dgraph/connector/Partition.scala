package uk.co.gresearch.spark.dgraph.connector

import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import org.apache.spark.sql.connector.read.InputPartition

/**
 * Partition of DGraph data. Reads all triples with the given predicates.
 * @param targets DGraph alpha servers
 * @param schema schema to read
 */
case class Partition(targets: Seq[Target], schema: Schema) extends InputPartition {

  // TODO: use host names of DGraph alphas to co-locate partitions
  override def preferredLocations(): Array[String] = super.preferredLocations()

  /**
   * Reads the entire partition and returns all triples.
   * @return triples
   */
  def getTriples: Iterator[Triple] = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val query = Query.forAllPropertiesAndEdges("data", schema)
      val client: DgraphClient = getClientFromChannel(channels)
      val response: Response = client.newReadOnlyTransaction().query(query)
      val json: String = response.getJson.toStringUtf8
      TriplesFactory.fromJson(json, "data", Some(schema))
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
