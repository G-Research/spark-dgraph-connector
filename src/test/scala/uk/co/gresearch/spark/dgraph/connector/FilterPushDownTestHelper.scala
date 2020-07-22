package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.scalatest.Assertions
import uk.co.gresearch.spark.dgraph.connector.partitioner.PredicatePartitioner

trait FilterPushDownTestHelper extends Assertions {

  def doTestFilterPushDownDf[T](ds: Dataset[T],
                                condition: Column,
                                expectedFilters: Set[Filter],
                                expectedUnpushed: Seq[Expression] = Seq.empty,
                                expectedDs: Set[T] = Set.empty): Unit = {
    val conditionedDs = ds.where(condition)
    val plan = conditionedDs.queryExecution.optimizedPlan
    val relationNode = plan match {
      case filter: logical.Filter =>
        val unpushedFilters = getFilterNodes(filter.condition)
        assert(unpushedFilters.map(_.sql) === expectedUnpushed.map(_.sql))
        filter.child
      case _ =>
        assert(expectedUnpushed.isEmpty, "some unpushed filters expected but there none filters actually unpushed")
        plan
    }
    assert(relationNode.isInstanceOf[DataSourceV2ScanRelation])

    val relation = relationNode.asInstanceOf[DataSourceV2ScanRelation]
    assert(relation.scan.isInstanceOf[TripleScan])

    val scan = relation.scan.asInstanceOf[TripleScan]
    assert(scan.partitioner.isInstanceOf[PredicatePartitioner])

    val partitioner = scan.partitioner.asInstanceOf[PredicatePartitioner]
    assert(partitioner.filters === expectedFilters)

    val actual = conditionedDs.collect()
    assert(actual.toSet === expectedDs)
    assert(actual.length === expectedDs.size)
  }

  def getFilterNodes(node: Expression): Seq[Expression] = node match {
    case And(left, right) => getFilterNodes(left) ++ getFilterNodes(right)
    case _ => Seq(node)
  }

}
