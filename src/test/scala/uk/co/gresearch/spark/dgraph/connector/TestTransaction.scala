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

import io.dgraph.dgraph4j.shaded.com.google.protobuf.ByteString
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Mutation
import io.dgraph.dgraph4j.shaded.io.grpc.ManagedChannel
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector.sources.TriplesSourceExpecteds
import uk.co.gresearch.spark.dgraph.connector.sources.TestTriplesSource.removeDgraphTriples

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

class TestTransaction extends AnyFunSpec with ConnectorSparkTestSession with DgraphTestCluster {

  import spark.implicits._

  // we want a fresh cluster that we can mutate, definitively not one that is always running and used by all tests
  override val clusterAlwaysStartUp: Boolean = true

  // for debugging purposes, write triples to files
  val writeTriplesFiles = false

  describe("Connector") {

    def mutate(): Unit = {
      val channels: Seq[ManagedChannel] = Seq(toChannel(Target(dgraph.target)))
      try {
        val client: DgraphClient = getClientFromChannel(channels)

        val mutationInsert = Mutation
          .newBuilder()
          .setSetNquads(
            ByteString.copyFromUtf8(
              s"""
               |_:1 <dgraph.type> "Person" .
               |_:1 <name> "Obi-Wan 'Ben' Kenobi" .
               |<0x${dgraph.sw1.toHexString}> <starring> _:1 .
               |""".stripMargin
            )
          )
          .setCommitNow(true)
          .build()

        val mutationUpdate = Mutation
          .newBuilder()
          .setSetNquads(
            ByteString.copyFromUtf8(
              s"""<0x${dgraph.leia.toHexString}> <name> "Princess Leia Organa" .""".stripMargin
            )
          )
          .setCommitNow(true)
          .build()

        val mutationDelete = Mutation
          .newBuilder()
          .setDelNquads(
            ByteString.copyFromUtf8(
              s"""<0x${dgraph.sw1.toHexString}> <starring> <0x${dgraph.leia.toHexString}> .""".stripMargin
            )
          )
          .setCommitNow(true)
          .build()

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

    def toString(triple: TypedTriple): String = {
      val value = triple.objectType match {
        case "uid"       => s"<${triple.objectUid.get}>"
        case "string"    => s""""${triple.objectString.get}""""
        case "long"      => triple.objectLong.get.toString
        case "double"    => triple.objectDouble.get.toString
        case "timestamp" => s""""${triple.objectTimestamp.get}""""
        case "boolean"   => triple.objectBoolean.get.toString
        case "geo"       => s""""${triple.objectGeo.get}""""
        case "password"  => "********"
        case "default"   => "default"
        case _           => "unknown"
      }
      s"<${triple.subject}> <${triple.predicate}> $value ."
    }

    def writeTriples(file: String, triples: Set[TypedTriple]): Unit = {
      if (writeTriplesFiles) {
        val content = triples.toList.map(toString).sorted.mkString("\n")
        Files.write(Paths.get(file), content.getBytes(StandardCharsets.UTF_8))
      }
    }

    it("should read in transaction") {
      // the graph before any mutations, read with transaction ...
      val beforeWithTransaction = removeDgraphTriples(
        reader.option(TransactionModeOption, TransactionModeReadOption).dgraph.triples(dgraph.target)
      )
      val beforeWithTransactionTriples = beforeWithTransaction.as[TypedTriple].collect().toSet
      assert(beforeWithTransactionTriples === TriplesSourceExpecteds(dgraph).getExpectedTypedTriples)
      // ... and without transaction
      val beforeWithoutTransaction = removeDgraphTriples(
        reader.option(TransactionModeOption, TransactionModeNoneOption).dgraph.triples(dgraph.target)
      )
      val beforeWithoutTransactionTriples = beforeWithoutTransaction.as[TypedTriple].collect().toSet
      assert(beforeWithoutTransactionTriples === TriplesSourceExpecteds(dgraph).getExpectedTypedTriples)

      // a mutation occurs
      mutate()

      // create another dataframe after mutation
      val after = removeDgraphTriples(reader.dgraph.triples(dgraph.target))
      val afterTriples = after.as[TypedTriple].collect().toSet

      // construct expected dataframe from materialized before triples
      val expectedAfterTriples =
        beforeWithTransactionTriples
          // minus updated
          .filterNot(t => t.subject == dgraph.leia && t.predicate == "name")
          // minus deleted
          .filterNot(t => t.subject == dgraph.sw1 && t.predicate == "starring" && t.objectUid.contains(dgraph.leia)) ++
          // plus
          Seq(
            // plus updated
            beforeWithTransactionTriples
              .find(t => t.subject == dgraph.leia && t.predicate == "name")
              .map(_.copy(objectString = Some("Princess Leia Organa")))
              .get,
            // plus inserted
            TypedTriple(
              dgraph.highestUid + 1,
              "dgraph.type",
              None,
              Some("Person"),
              None,
              None,
              None,
              None,
              None,
              None,
              "string"
            ),
            TypedTriple(
              dgraph.highestUid + 1,
              "name",
              None,
              Some("Obi-Wan 'Ben' Kenobi"),
              None,
              None,
              None,
              None,
              None,
              None,
              "string"
            ),
            TypedTriple(
              dgraph.sw1,
              "starring",
              Some(dgraph.highestUid + 1),
              None,
              None,
              None,
              None,
              None,
              None,
              None,
              "uid"
            )
          ).toSet
      writeTriples("after-mutations-expected.txt", expectedAfterTriples)

      writeTriples("before-mutations.txt", beforeWithoutTransactionTriples)

      // changes in the dataframe without transaction
      val afterBeforeWithoutTransactionTriples = beforeWithoutTransaction.as[TypedTriple].collect().toSet
      writeTriples("after-mutations-without-transaction.txt", afterBeforeWithoutTransactionTriples)
      assert(afterBeforeWithoutTransactionTriples !== beforeWithoutTransactionTriples)
      assert(afterBeforeWithoutTransactionTriples === expectedAfterTriples)

      // same content as in dataframe created after mutation
      writeTriples("after-mutations.txt", afterTriples)
      assert(afterTriples !== beforeWithTransactionTriples)
      assert(afterTriples === expectedAfterTriples)

      // no change in the dataframe with transaction
      val afterBeforeWithTransactionTriples = beforeWithTransaction.as[TypedTriple].collect().toSet
      writeTriples("after-mutations-with-transaction.txt", afterBeforeWithTransactionTriples)
      // Dgraph v25.3.x has a regression breaking transaction isolation
      // see https://github.com/dgraph-io/dgraph/issues/9795
      if (!dgraph.clusterVersion.startsWith("25.3.")) {
        assert(afterBeforeWithTransactionTriples === beforeWithTransactionTriples)
      }
    }
  }
}
