package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{Partition, Uid, UidRange, Uids}

class TestUidCardinalityEstimator extends FunSpec {

  def doTestUidCardinalityEstimatorBase(estimator: UidCardinalityEstimatorBase,
                                        expectedEstimationWithoutRange: Option[Long]): Unit = {

    it("should estimate partition's uid range") {
      val range = UidRange(Uid(1), Uid(1000))
      val partition = Partition(Seq.empty, Set(range))
      val actual = estimator.uidCardinality(partition)
      assert(actual.isDefined === true)
      assert(actual.get === range.length)
    }

    it("should estimate partition's uids cardinality") {
      val uids = Uids(Set(Uid(1), Uid(2), Uid(3)))
      val partition = Partition(Seq.empty, Set(uids))
      val actual = estimator.uidCardinality(partition)
      assert(actual.isDefined === true)
      assert(actual.get === uids.uids.size)
    }

    it("should estimate partition without uid range and uids") {
      val partition = Partition(Seq.empty)
      val actual = estimator.uidCardinality(partition)
      assert(actual === expectedEstimationWithoutRange)
    }

  }

  describe("UidCardinalityEstimatorBase") {
    doTestUidCardinalityEstimatorBase(new UidCardinalityEstimatorBase {}, None)
  }

  describe("MaxLeaseIdUidCardinalityEstimator") {
    val estimator = MaxLeaseIdUidCardinalityEstimator(1234)
    doTestUidCardinalityEstimatorBase(estimator, Some(1234))

    it("should fail on negative or zero max uids") {
      assertThrows[IllegalArgumentException] {
        UidCardinalityEstimator.forMaxLeaseId(-1)
      }
      assertThrows[IllegalArgumentException] {
        UidCardinalityEstimator.forMaxLeaseId(0)
      }
    }

  }

}
