/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import org.apache.spark.sql.connector.read.InputPartition

/**
 * Partition of Dgraph data. Reads all triples with the given predicates in the given uid range.
 *
 * @param targets Dgraph alpha nodes
 * @param predicates optional predicates to read
 * @param uids optional uid ranges
 */
case class Partition(targets: Seq[Target], predicates: Option[Set[Predicate]], uids: Option[UidRange]) extends InputPartition {

  // TODO: use host names of Dgraph alphas to co-locate partitions
  override def preferredLocations(): Array[String] = super.preferredLocations()

  /**
   * Reads the entire partition and returns all triples.
   *
   * @param triplesFactory: triples factory
   * @return triples
   */
  def getTriples(triplesFactory: TriplesFactory): Iterator[Triple] = {
    val query =
      predicates
        .map(Query.forPropertiesAndEdges("data", _, uids))
        .getOrElse(Query.forAllPropertiesAndEdges("data", uids))

    readTriples(query, None, triplesFactory)
  }

  /**
   * Reads the entire partition and returns all edge triples.
   *
   * @param triplesFactory: triples factory
   * @return triples
   */
  def getEdgeTriples(triplesFactory: TriplesFactory): Iterator[Triple] = {
    val query =
      predicates
        // returns only edges due to schema
        .map(Query.forPropertiesAndEdges("data", _, uids))
        // returns properties and edges, requires filtering for edges (see below)
        .getOrElse(Query.forAllPropertiesAndEdges("data", uids))

    val nodeTriplesFilter: Option[Triple => Boolean] =
      predicates
        // no filtering
        .map(_ => None)
        // filter for true edges
        .getOrElse(Some((t: Triple) => t.o.isInstanceOf[Uid]))

    readTriples(query, nodeTriplesFilter, triplesFactory)
  }

  /**
   * Reads the entire partition and returns all node triples.
   *
   * @param triplesFactory: triples factory
   * @return triples
   */
  def getNodeTriples(triplesFactory: TriplesFactory): Iterator[Triple] = {
    val query =
      predicates
        .map(Query.forPropertiesAndEdges("data", _, uids))
        .getOrElse(Query.forAllProperties("data", uids))
    readTriples(query, None, triplesFactory)
  }

  /**
   * Sends the query, parses the Json response into triples and filters with the optional filter.
   * @param query dgraph query
   * @param triplesFilter optional filter for triples
   * @return triples
   */
  private def readTriples(query: String, triplesFilter: Option[Triple => Boolean], triplesFactory: TriplesFactory): Iterator[Triple] = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val client: DgraphClient = getClientFromChannel(channels)
      val response: Response = client.newReadOnlyTransaction().query(query)
      val json: String = response.getJson.toStringUtf8
      val triples = triplesFactory.fromJson(json, "data")
      triplesFilter.map(triples.filter).getOrElse(triples)
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
