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

import io.dgraph.DgraphProto.TxnContext
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedTripleEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.TripleTableModel

import scala.collection.JavaConverters._

class TestPartition extends AnyFunSpec with SchemaProvider with DgraphTestCluster {

  describe("Partition") {

    Seq(10, 100, 1000, 10000).foreach { predicates =>

      // test that a partition works with N predicates
      // the query grows linearly with N, so does the processing time
      it(s"should read $predicates predicates") {
        val options = new CaseInsensitiveStringMap(Map(IncludeReservedPredicatesOption -> "dgraph.type").asJava)
        val targets = Seq(Target(dgraph.target))
        val existingPredicates = getSchema(targets, options).predicates.slice(0, predicates)
        val syntheticPredicates =
          (1 to (predicates - existingPredicates.size)).map(pred =>
            Predicate(s"predicate$pred", if (pred % 2 == 0) "string" else "uid")
          ).toSet
        val schema = Schema(syntheticPredicates ++ existingPredicates)
        val encoder = TypedTripleEncoder(schema.predicateMap)
        val transaction = Some(Transaction(TxnContext.newBuilder().build()))
        val execution = DgraphExecutorProvider(transaction)
        val model = TripleTableModel(execution, encoder, ChunkSizeDefault)
        val partition = Partition(targets).has(schema.predicates).langs(existingPredicates.filter(_.isLang).map(_.predicateName))
        val rows = model.modelPartition(partition).toList
        val dgraphNodeUids = rows.filter(row => row.getString(1) == "dgraph.type" && row.getString(3).startsWith("dgraph.")).map(_.getLong(0)).toSet
        val res = rows.filter(rows => !dgraphNodeUids.contains(rows.getLong(0)))
        assert(res.length === 61)
      }

    }

    it("should return partition query") {
      val ops =
        Set[Operator](
          Has(Set("pred"), Set.empty),
          UidRange(Uid(10), Uid(20)),
          IsIn("pred", Set[Any]("value"))
        )

      val partition = Partition(Seq(Target("localhost:9080")), ops)
      val query = partition.query
      assert(query === PartitionQuery("result", ops))
    }

  }

}
