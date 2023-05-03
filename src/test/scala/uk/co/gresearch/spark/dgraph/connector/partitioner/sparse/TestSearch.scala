package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import com.google.common.primitives.UnsignedLong
import org.scalatest.funspec.AnyFunSpec

class TestSearch extends AnyFunSpec {
  describe("BinarySearch") {
    Seq(
      (1, 1, 1, Seq()),
      (1, 2, 1, Seq(1)),
      (1, 2, 2, Seq(1)),
      (1, 3, 1, Seq(2, 1)),
      (1, 3, 2, Seq(2)),
      (1, 3, 3, Seq(2)),
      (1, 4, 1, Seq(2, 1)),
      (1, 4, 2, Seq(2)),
      (1, 4, 3, Seq(2, 3)),
      (1, 4, 4, Seq(2, 3)),
      (0, 20, 10, Seq(10)),
      (10, 20, 12, Seq(15, 12)),
      (10, 20, 18, Seq(15, 18)),
      (10, 20, 13, Seq(15, 12, 14, 13)),
      (10, 20, 17, Seq(15, 18, 17)),
      (0, 100, 17, Seq(50, 25, 12, 19, 16, 18, 17)),
      (0, 1000, 998, Seq(500, 750, 875, 938, 969, 985, 993, 997, 999, 998)),
    ).foreach { case (left, right, target, trajectory) =>
      it(s"should search for $target in [$left…$right]") {
        val strategy = BinarySearchStrategy(0)
        val search = SearchWithStrategy[Trajectory, Result](strategy)
        val result = search.search(Step(left, right, target))
        assert(result.result.longValue() === target)
        assert(result.trajectory.steps.map(_.longValue()) === trajectory)
      }
    }

    it(s"should search with range width threshold") {
      val left = 0
      val right = 10000
      val threshold = 10
      val strategy = BinarySearchStrategy(threshold)
      val search = SearchWithStrategy[Trajectory, Result](strategy)

      left.to(right).foreach { target =>
        val result = search.search(Step(left, right, target))
        assert((target - result.result.longValue()).abs <= threshold, s"target=$target result=${result.result.longValue()}")
      }

      (left+1).to(right).foreach { target =>
        val result = search.search(Step(left+1, right, target))
        assert((target - result.result.longValue()).abs <= threshold, s"target=$target result=${result.result.longValue()}")
      }
    }
  }

  describe("LogSearch") {
    Seq(
      (1, 1, 1, 1, Seq()),
      (1, 2, 1, 1, Seq(1)),
      (1, 2, 2, 2, Seq(1)),
      (1, 3, 1, 1, Seq(1)),
      (1, 3, 2, 2, Seq(1)),
      (1, 3, 3, 2, Seq(1)),
      (1, 4, 1, 1, Seq(3, 1)),
      (1, 4, 2, 2, Seq(3, 1)),
      (1, 4, 3, 3, Seq(3)),
      (1, 4, 4, 4, Seq(3)),
      (0, 20, 10, 8, Seq(3, 15, 7)),
      (10, 20, 12, 10, Seq(15)),
      (10, 20, 18, 16, Seq(15)),
      (10, 20, 13, 10, Seq(15)),
      (10, 20, 17, 16, Seq(15)),
      (0, 100, 17, 16, Seq(7, 31, 15)),
      (0, 1000, 998, 512, Seq(31, 255, 511)),
      (0, 1000000, 3, 3, Seq(1023, 31, 3)),
      (0, 1000000, 998, 512, Seq(1023, 31, 255, 511)),
    ).foreach { case (left, right, target, expected, trajectory) =>
      it(s"should search for $target in [$left…$right]") {
        val strategy = LogSearchStrategy()
        val search = SearchWithStrategy[Trajectory, Result](strategy)
        val result = search.search(Step(left, right, target))
        assert(result.result.longValue() === expected)
        assert(result.trajectory.steps.map(_.longValue()) === trajectory)
      }
    }
  }


  case class Step(override val left: UnsignedLong,
                  override val right: UnsignedLong,
                  target: UnsignedLong,
                  override val payload: Trajectory = Trajectory()) extends SearchStep[Trajectory, Result] {
    override def move(middle: UnsignedLong): SearchStep[Trajectory, Result] = {
      val cmp = target.compareTo(middle)
      if (cmp < 0) {
        copy(left = left, right = middle, payload = payload.add(middle))
      } else if (cmp > 0) {
        copy(left = middle.plus(UnsignedLong.ONE), right = right, payload = payload.add(middle))
      } else {
        copy(left = middle, right = middle, payload = payload.add(middle))
      }
    }
    override def result(): Result = Result(left, payload)
  }

  object Step {
    def apply(left: Long, right: Long, target: Long): Step =
      Step(UnsignedLong.valueOf(left), UnsignedLong.valueOf(right), UnsignedLong.valueOf(target))
  }

  case class Trajectory(steps: Seq[UnsignedLong] = Seq.empty) {
    def add(step: UnsignedLong): Trajectory = copy(steps = steps :+ step)
  }

  case class Result(result: UnsignedLong, trajectory: Trajectory)
}
