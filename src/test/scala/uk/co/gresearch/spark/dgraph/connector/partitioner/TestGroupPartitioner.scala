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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.util.UUID

import io.dgraph.DgraphProto.TxnContext
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedTripleEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.TripleTableModel

class TestGroupPartitioner extends FunSpec {

  val transaction: Transaction = Transaction(TxnContext.newBuilder().build())

  describe("GroupPartitioner") {

    val schema = Schema((1 to 4).map(i => Predicate(s"pred$i", s"type$i", s"type$i")).toSet)
    val clusterState = ClusterState(
      Map(
        "1" -> Set(Target("host1:9080")),
        "2" -> Set(Target("host2:9080"), Target("host3:9080")),
        "3" -> Set(Target("host4:9080"))
      ),
      Map(
        "1" -> Set.empty,
        "2" -> Set("pred1", "pred2", "pred3"),
        "3" -> Set("pred4")
      ),
      10000,
      UUID.randomUUID()
    )
    val execution = DgraphExecutorProvider(transaction)
    val encoder = TypedTripleEncoder(schema.predicateMap)
    implicit val model: TripleTableModel = TripleTableModel(execution, encoder, ChunkSizeDefault)

    it("should partition") {
      val partitioner = GroupPartitioner(schema, clusterState)
      val partitions = partitioner.getPartitions(model)

      assert(partitions.length === 2)
      assert(partitions.toSet === Set(
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set(Predicate("pred1", "type1", "type1"), Predicate("pred2", "type2", "type2"), Predicate("pred3", "type3", "type3"))),
        Partition(Seq(Target("host4:9080"))).has(Set(Predicate("pred4", "type4", "type4")))
      ))
    }

  }

}
