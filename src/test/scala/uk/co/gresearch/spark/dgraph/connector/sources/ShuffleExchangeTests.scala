package uk.co.gresearch.spark.dgraph.connector.sources

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funspec.AnyFunSpec

trait ShuffleExchangeTests {
  this: AnyFunSpec =>

  def containsShuffleExchangeExec(plan: SparkPlan): Boolean = plan match {
    case p: AdaptiveSparkPlanExec => containsShuffleExchangeExec(p.executedPlan)
    case _: ShuffleExchangeExec => true
    case p => p.children.exists(containsShuffleExchangeExec)
  }

  def testForShuffleExchange(df: () => DataFrame,
                             tests: Seq[(String, DataFrame => DataFrame, () => Seq[Row])],
                             shuffleExpected: Boolean): Unit = {
    val label = if (shuffleExpected) "shuffle" else "reuse partitioning"
    tests.foreach {
      case (test, op, expected) =>
        it(s"should $label for $test") {
          val data = op(df())
          val plan = data.queryExecution.executedPlan
          assert(containsShuffleExchangeExec(plan) === shuffleExpected, plan)
          assert(data.sort(data.columns.map(col): _*).collect() === expected())
        }
    }
  }

}
