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
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedTripleEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.TripleTableModel
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Filters, ObjectTypeIsIn, ObjectValueIsIn, Partition, Predicate, Schema, SubjectIsIn, Target, Uid, UidRange, _}

class TestUidRangePartitioner extends AnyFunSpec {

  describe("UidRangePartitioner") {

    val schema = Schema(Set(Predicate("pred1", "type1", "type1"), Predicate("pred2", "type2", "type2")))
    val clusterState = ClusterState(
      Map(
        "1" -> Set(Target("host1:9080"), Target("host2:9080")),
        "2" -> Set(Target("host3:9080"))
      ),
      Map(
        "1" -> Set("pred1"),
        "2" -> Set("pred2")
      ),
      10000,
      UUID.randomUUID()
    )
    val execution = DgraphExecutorProvider(None)
    val encoder = TypedTripleEncoder(schema.predicateMap)
    implicit val model: TripleTableModel = TripleTableModel(execution, encoder, ChunkSizeDefault)

    val singleton = SingletonPartitioner(Seq(Target("host1:9080"), Target("host2:9080"), Target("host3:9080")), schema)
    val group = GroupPartitioner(schema, clusterState)
    val alpha = AlphaPartitioner(schema, clusterState, 1)
    val pred = PredicatePartitioner(schema, clusterState, 1)

    val testPartitioners =
      Seq(
        (singleton, "singleton"),
        (group, "group"),
        (alpha, "alpha"),
        (pred, "predicates"),
      )

    val testSizes = Seq(2000, 5000)

    testPartitioners.foreach { case (partitioner, label) =>
      testSizes.foreach { size =>

        it(s"should decorate $label partitioner with ${size} uids per partition") {
          val uidPartitioner = UidRangePartitioner(partitioner, size, UidCardinalityEstimator.forMaxLeaseId(clusterState.maxLeaseId))
          val partitions = partitioner.getPartitions
          val uidPartitions = uidPartitioner.getPartitions
          println(uidPartitions)

          val ranges = (0 until (10000 / size)).map(idx => UidRange(Uid(1 + idx * size), Uid(1 + (idx+1) * size)))
          assert(uidPartitions.length === partitions.length * ranges.length)
          val expectedPartitions = partitions.flatMap( partition =>
            ranges.zipWithIndex.map { case (range, idx) =>
              Partition(partition.targets.rotateLeft(idx), partition.operators ++ Set(range))
            }
          )

          assert(uidPartitions === expectedPartitions)
        }

      }

    }

    testPartitioners.foreach{ case (partitioner, label) =>

      it(s"should noop $label partitioner with too large uidsPerPartition") {
        val uidPartitioner = UidRangePartitioner(partitioner, clusterState.maxLeaseId.toInt, UidCardinalityEstimator.forMaxLeaseId(clusterState.maxLeaseId))
        assert(uidPartitioner.getPartitions === partitioner.getPartitions)
      }

    }

    it("should fail on null partitioner") {
      assertThrows[IllegalArgumentException] {
        UidRangePartitioner(null, 1, UidCardinalityEstimator.forMaxLeaseId(1000))
      }
    }

    it("should fail with integer overflow partition size") {
      assertThrows[IllegalArgumentException] {
        UidRangePartitioner(singleton, 1, UidCardinalityEstimator.forMaxLeaseId(Long.MaxValue)).getPartitions
      }
    }

    it("should fail on decorating uid partitioner") {
      val uidPartitioner = UidRangePartitioner(singleton, 2, UidCardinalityEstimator.forMaxLeaseId(1000))
      assertThrows[IllegalArgumentException] {
        UidRangePartitioner(uidPartitioner, 1, UidCardinalityEstimator.forMaxLeaseId(1000)).getPartitions
      }
    }

    it("should support what decorated partitioner supports") {
      val partitioner = PredicatePartitioner(schema, clusterState, 1)
      val uidPartitioner = UidRangePartitioner(partitioner, 2, UidCardinalityEstimator.forMaxLeaseId(1000))

      Seq[Set[Filter]](
        Set(SubjectIsIn(Uid("0x1"))),
        Set(IntersectPredicateNameIsIn("pred")),
        Set(IntersectPredicateNameIsIn("pred"), ObjectValueIsIn("value")),
        Set(PredicateNameIs("pred")),
        Set(PredicateNameIs("pred"), ObjectValueIsIn("value")),
        Set(IntersectPredicateValueIsIn(Set("pred"), Set("value"))),
        Set(SinglePredicateValueIsIn("pred", Set("value"))),
        Set(ObjectTypeIsIn("type")),
        Set(ObjectValueIsIn("type"))
      ).foreach {
        filters =>
          val actual = uidPartitioner.supportsFilters(filters)
          val expected = partitioner.supportsFilters(filters)
          assert(actual === expected, filters)
      }
    }

    it("should forward filters to decorated partitioner") {
      val partitioner = PredicatePartitioner(schema, clusterState, 1)
      val uidPartitioner = UidRangePartitioner(partitioner, 2, UidCardinalityEstimator.forMaxLeaseId(1000))
      // deliberately not EmptyFilters here to test with our own instance
      val filters = Filters(Set.empty, Set.empty)
      val actual =
        uidPartitioner.withFilters(filters)
          .partitioner.asInstanceOf[connector.partitioner.PredicatePartitioner]
          .filters
      assert(actual eq filters)
    }

    it("should forward projection to decorated partitioner") {
      val partitioner = PredicatePartitioner(schema, clusterState, 1)
      val uidPartitioner = UidRangePartitioner(partitioner, 2, UidCardinalityEstimator.forMaxLeaseId(1000))
      val projection = schema.predicates.toSeq
      val actual =
        uidPartitioner.withProjection(projection)
          .partitioner.asInstanceOf[connector.partitioner.PredicatePartitioner]
          .projection
      assert(actual.isDefined === true)
      assert(actual.get eq projection)
    }

  }

}
