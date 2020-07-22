package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanExec}
import org.apache.spark.sql.{Column, Dataset, execution}
import org.scalatest.Assertions
import uk.co.gresearch.spark.dgraph.connector.partitioner.PredicatePartitioner

trait FilterPushDownTestHelper extends Assertions {

  def doTestFilterPushDownDf[T](ds: Dataset[T],
                                condition: Column,
                                expectedFilters: Set[Filter],
                                expectedUnpushed: Seq[Expression] = Seq.empty,
                                expectedDs: Set[T] = Set.empty): Unit = {
    val conditionedDs = ds.where(condition)
    assert(conditionedDs.queryExecution.sparkPlan.isInstanceOf[ProjectExec])
    val plan = conditionedDs.queryExecution.sparkPlan.asInstanceOf[ProjectExec].child
    val root = plan match {
      case filter: execution.FilterExec =>
        val unpushedFilters = getFilterNodes(filter.condition)
        assert(unpushedFilters.map(_.sql) === expectedUnpushed.map(_.sql))
        filter.child
      case _ =>
        assert(expectedUnpushed.isEmpty, "some unpushed filters expected but there none filters actually unpushed")
        plan
    }
    assert(root.isInstanceOf[DataSourceV2ScanExec])

    val scan = root.asInstanceOf[DataSourceV2ScanExec]
    assert(scan.reader.isInstanceOf[TripleScan])

    val reader = scan.reader.asInstanceOf[TripleScan]
    assert(reader.filters === expectedFilters)

    val actual = conditionedDs.collect()
    assert(actual.toSet === expectedDs)
    assert(actual.length === expectedDs.size)
  }

  def getFilterNodes(node: Expression): Seq[Expression] = node match {
    case And(left, right) => getFilterNodes(left) ++ getFilterNodes(right)
    case _ => Seq(node)
  }

}
