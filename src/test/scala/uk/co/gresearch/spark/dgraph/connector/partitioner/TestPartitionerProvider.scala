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

import com.google.common.primitives.UnsignedLong
import io.dgraph.DgraphProto.TxnContext
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.partitioner.sparse.UidRangeDetector

import java.util.UUID
import scala.jdk.CollectionConverters._

class TestPartitionerProvider extends AnyFunSpec {

  val target = Seq(Target("localhost:9080"))
  val schema: Schema = Schema(Set(Predicate("pred", "string")))
  val state: ClusterState = ClusterState(
    Map("1" -> target.toSet),
    Map("1" -> schema.predicates.map(_.predicateName)),
    Some(UnsignedLong.valueOf(10000)),
    UUID.randomUUID()
  )
  val transaction: Option[Transaction] = Some(Transaction(TxnContext.newBuilder().build()))
  val maxLeaseEstimator: UidCardinalityEstimator = MaxUidUidCardinalityEstimator(state.maxUid)
  assert(UidRangePartitionerEstimatorDefault === MaxUidEstimatorOption, "tests assume this default estimator")

  def detector(uidsPerPartition: Int): UidRangeDetector =
    new ConfigPartitionerOption().getUidRangeDetector(target, uidsPerPartition, CaseInsensitiveStringMap.empty())

  describe("PartitionerProvider") {

    val singleton = SingletonPartitioner(target, schema)
    val group = GroupPartitioner(schema, state)
    val alpha = AlphaPartitioner(schema, state, AlphaPartitionerPartitionsDefault)
    val pred = PredicatePartitioner(schema, state, PredicatePartitionerPredicatesDefault)
    val uidRange = UidRangePartitioner(
      singleton,
      UidRangePartitionerUidsPerPartDefault,
      UidRangePartitionerMaxPartsDefault,
      maxLeaseEstimator,
      detector(1000000)
    )

    Seq(
      ("singleton", singleton),
      ("group", group),
      ("alpha", alpha),
      ("predicate", pred),
      ("uid-range", uidRange),
      ("singleton+uid-range", uidRange),
      ("group+uid-range", uidRange.copy(partitioner = group)),
      ("alpha+uid-range", uidRange.copy(partitioner = alpha)),
      ("predicate+uid-range", uidRange.copy(partitioner = pred)),
    ).foreach { case (partOption, expected) =>
      it(s"should provide $partOption partitioner via option") {
        val provider = new PartitionerProvider {}
        val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> partOption).asJava)
        val partitioner = provider.getPartitioner(schema, state, transaction, options)
        assert(partitioner === expected)
      }

    }

    it("should provide default partitioner") {
      val provider = new PartitionerProvider {}
      val options = CaseInsensitiveStringMap.empty()
      val partitioner = provider.getPartitioner(schema, state, transaction, options)

      val predicatePart = PredicatePartitioner(schema, state, PredicatePartitionerPredicatesDefault)
      val expected = UidRangePartitioner(
        predicatePart,
        UidRangePartitionerUidsPerPartDefault,
        UidRangePartitionerMaxPartsDefault,
        maxLeaseEstimator,
        detector(1000000)
      )
      assert(partitioner === expected)
    }

    it("should provide configurable default partitioner") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(
        Map(
          PredicatePartitionerPredicatesOption -> "1",
          UidRangePartitionerUidsPerPartOption -> "2",
          UidRangePartitionerMaxPartsOption -> "3",
          UidRangePartitionerEstimatorOption -> MaxUidEstimatorOption
        ).asJava
      )
      val partitioner = provider.getPartitioner(schema, state, transaction, options)

      val predicatePart = PredicatePartitioner(schema, state, 1)
      val expected = UidRangePartitioner(predicatePart, 2, 3, MaxUidUidCardinalityEstimator(state.maxUid), detector(2))
      assert(partitioner === expected)
    }

    it("should fail on unknown partitioner option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> "unknown").asJava)
      assertThrows[IllegalArgumentException] {
        provider.getPartitioner(schema, state, transaction, options)
      }
    }

    it("should fail on unknown uidRange partitioner option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> "unknown+uid-range").asJava)
      assertThrows[IllegalArgumentException] {
        provider.getPartitioner(schema, state, transaction, options)
      }
    }

    it(s"should provide alpha partitioner with non-default partsPerAlpha via option") {
      val provider = new PartitionerProvider {}
      val options =
        new CaseInsensitiveStringMap(Map(PartitionerOption -> "alpha", AlphaPartitionerPartitionsOption -> "2").asJava)
      val partitioner = provider.getPartitioner(schema, state, transaction, options)
      assert(partitioner === alpha.copy(partitionsPerAlpha = 2))
    }

    it(s"should provide predicate partitioner with non-default predsPerPart via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(
        Map(PartitionerOption -> "predicate", PredicatePartitionerPredicatesOption -> "2").asJava
      )
      val partitioner = provider.getPartitioner(schema, state, transaction, options)
      assert(partitioner === pred.copy(predicatesPerPartition = 2))
    }

    it(s"should provide uid-range partitioner with non-default values via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(
        Map(
          PartitionerOption -> "uid-range",
          UidRangePartitionerUidsPerPartOption -> "2",
          UidRangePartitionerEstimatorOption -> MaxUidEstimatorOption,
        ).asJava
      )
      val partitioner = provider.getPartitioner(schema, state, transaction, options)
      assert(
        partitioner === uidRange.copy(
          uidsPerPartition = 2,
          uidCardinalityEstimator = maxLeaseEstimator,
          uidRangeDetector = detector(2)
        )
      )
    }

    it(s"should provide alpha uid-range partitioner with non-default values via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(
        Map(
          PartitionerOption -> "alpha+uid-range",
          AlphaPartitionerPartitionsOption -> "2",
          UidRangePartitionerUidsPerPartOption -> "2",
          UidRangePartitionerEstimatorOption -> MaxUidEstimatorOption,
        ).asJava
      )
      val partitioner = provider.getPartitioner(schema, state, transaction, options)
      assert(
        partitioner === uidRange.copy(
          partitioner = alpha.copy(partitionsPerAlpha = 2),
          uidsPerPartition = 2,
          uidCardinalityEstimator = maxLeaseEstimator,
          uidRangeDetector = detector(2)
        )
      )
    }

    it(s"should provide predicate partitioner with non-default values via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(
        Map(
          PartitionerOption -> "predicate+uid-range",
          PredicatePartitionerPredicatesOption -> "2",
          UidRangePartitionerUidsPerPartOption -> "2",
          UidRangePartitionerEstimatorOption -> MaxUidEstimatorOption,
        ).asJava
      )
      val partitioner = provider.getPartitioner(schema, state, transaction, options)
      assert(
        partitioner === uidRange.copy(
          partitioner = pred.copy(predicatesPerPartition = 2),
          uidsPerPartition = 2,
          uidCardinalityEstimator = maxLeaseEstimator,
          uidRangeDetector = detector(2)
        )
      )
    }

  }

}
