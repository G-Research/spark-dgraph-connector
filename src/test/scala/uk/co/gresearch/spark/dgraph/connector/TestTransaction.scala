/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import com.google.protobuf.ByteString
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Mutation
import io.grpc.ManagedChannel
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector.sources.{TestTriplesSource, TriplesSourceExpecteds}

class TestTransaction extends FunSpec with SparkTestSession with DgraphTestCluster {

  import spark.implicits._

  // we want a fresh cluster that we can mutate, definitively not one that is always running and used by all tests
  override val clusterAlwaysStartUp: Boolean = true

  describe("Connector") {

    def mutate(): Unit = {
      val channels: Seq[ManagedChannel] = Seq(toChannel(Target(dgraph.target)))
      try {
        val client: DgraphClient = getClientFromChannel(channels)

        val mutationInsert = Mutation.newBuilder()
          .setSetNquads(ByteString.copyFromUtf8(
            s"""
               |_:1 <dgraph.type> "Person" .
               |_:1 <name> "Obi-Wan 'Ben' Kenobi" .
               |<0x${dgraph.sw1.toHexString}> <starring> _:1 .
               |""".stripMargin
          ))
          .setCommitNow(true).build()

        val mutationUpdate = Mutation.newBuilder()
          .setSetNquads(ByteString.copyFromUtf8(
            s"""<0x${dgraph.leia.toHexString}> <name> "Princess Leia Organa" .""".stripMargin
          ))
          .setCommitNow(true).build()

        val mutationDelete = Mutation.newBuilder()
          .setDelNquads(ByteString.copyFromUtf8(
            s"""<0x${dgraph.sw1.toHexString}> <starring> <0x${dgraph.leia.toHexString}> .""".stripMargin
          ))
          .setCommitNow(true).build()

        val transactionInsert = client.newTransaction()
        transactionInsert.mutate(mutationInsert)
        transactionInsert.close()

        val transactionUpdate = client.newTransaction()
        transactionUpdate.mutate(mutationUpdate)
        transactionUpdate.close()

        val transactionDelete = client.newTransaction()
        transactionDelete.mutate(mutationDelete)
        transactionDelete.close()
      } finally {
        channels.foreach(_.shutdown())
      }
    }

    it("should read in transaction") {
      val before = spark.read.dgraph.triples(dgraph.target)
      val beforeTriples = before.as[TypedTriple].collect().toSet
      assert(beforeTriples === TriplesSourceExpecteds(dgraph).getExpectedTypedTriples)

      mutate()
      val afterBeforeTriples = before.as[TypedTriple].collect().toSet
      assert(beforeTriples === afterBeforeTriples)

      val after = spark.read.dgraph.triples(dgraph.target)
      val afterTriples = after.as[TypedTriple].collect().toSet

      val expectedAfterTriples =
        beforeTriples
          // minus updated
          .filterNot(t => t.subject == dgraph.leia && t.predicate == "name")
          // minus deleted
          .filterNot(t => t.subject == dgraph.sw1 && t.predicate == "starring" && t.objectUid.contains(dgraph.leia)) ++
          // plus
          Seq(
            // plus updated
            beforeTriples
              .find(t => t.subject == dgraph.leia && t.predicate == "name")
              .map(_.copy(objectString = Some("Princess Leia Organa")))
              .get,
            // plus insterted
            TypedTriple(dgraph.highestUid + 1, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
            TypedTriple(dgraph.highestUid + 1, "name", None, Some("Obi-Wan 'Ben' Kenobi"), None, None, None, None, None, None, "string"),
            TypedTriple(dgraph.sw1, "starring", Some(dgraph.highestUid + 1), None, None, None, None, None, None, None, "uid")
          ).toSet

      assert(afterTriples !== beforeTriples)
      assert(afterTriples === expectedAfterTriples)
    }
  }
}
