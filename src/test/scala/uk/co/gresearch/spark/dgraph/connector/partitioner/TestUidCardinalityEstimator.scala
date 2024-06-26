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
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector._

class TestUidCardinalityEstimator extends AnyFunSpec {

  def doTestUidCardinalityEstimatorBase(
      estimator: UidCardinalityEstimatorBase,
      expectedEstimationWithoutRange: Option[Long]
  ): Unit = {

    it("should estimate partition's uid range") {
      val range = UidRange(Uid(1), Uid(1000))
      val partition = Partition(Seq.empty, Set(range))
      val actual = estimator.uidCardinality(partition)
      assert(actual.isDefined === true)
      assert(actual.get.intValue() === range.length.intValue())
    }

    it("should estimate partition's uids cardinality") {
      val uids = Uids(Set(Uid(1), Uid(2), Uid(3)))
      val partition = Partition(Seq.empty, Set(uids))
      val actual = estimator.uidCardinality(partition)
      assert(actual.isDefined === true)
      assert(actual.get.intValue() === uids.uids.size)
    }

    it("should estimate partition without uid range and uids") {
      val partition = Partition(Seq.empty)
      val actual = estimator.uidCardinality(partition)
      assert(actual.map(_.intValue()) === expectedEstimationWithoutRange)
    }

  }

  describe("UidCardinalityEstimatorBase") {
    doTestUidCardinalityEstimatorBase(new UidCardinalityEstimatorBase {}, None)
  }

  describe("MaxUidUidCardinalityEstimator") {
    describe("with some maxUid") {
      doTestUidCardinalityEstimatorBase(MaxUidUidCardinalityEstimator(Some(UnsignedLong.valueOf(1234))), Some(1234))
    }
    describe("with no maxUid") {
      doTestUidCardinalityEstimatorBase(MaxUidUidCardinalityEstimator(None), None)
    }

    it("should fail on negative or zero max uids") {
      assertThrows[IllegalArgumentException] {
        UidCardinalityEstimator.forMaxUid(Some(UnsignedLong.valueOf(-1)))
      }
      assertThrows[IllegalArgumentException] {
        UidCardinalityEstimator.forMaxUid(Some(UnsignedLong.valueOf(0)))
      }
    }

  }

}
