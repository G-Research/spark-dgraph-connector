package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.Assertions
import uk.co.gresearch.spark.dgraph.connector.partitioner.PredicatePartitioner

trait FilterPushDownTestHelper extends Assertions {

  def doTestFilterPushDownDf(df: DataFrame, condition: Column, expected: Seq[Filter], expectedUnpushed: Seq[Expression] = Seq.empty): Unit = {

    val plan = df.where(condition).queryExecution.optimizedPlan
    val relationNode = plan match {
      case filter: logical.Filter =>
        val unpushedFilters = getFilterNodes(filter.condition)
        assert(unpushedFilters.map(_.sql) === expectedUnpushed.map(_.sql))
        filter.child
      case _ =>
        assert(expectedUnpushed.isEmpty)
        plan
    }
    assert(relationNode.isInstanceOf[DataSourceV2ScanRelation])

    val relation = relationNode.asInstanceOf[DataSourceV2ScanRelation]
    assert(relation.scan.isInstanceOf[TripleScan])

    val scan = relation.scan.asInstanceOf[TripleScan]
    assert(scan.partitioner.isInstanceOf[PredicatePartitioner])

    val partitioner = scan.partitioner.asInstanceOf[PredicatePartitioner]
    assert(partitioner.filters.toSet === expected.toSet)
  }

  def getFilterNodes(node: Expression): Seq[Expression] = node match {
    case And(left, right) => getFilterNodes(left) ++ getFilterNodes(right)
    case _ => Seq(node)
  }

}
