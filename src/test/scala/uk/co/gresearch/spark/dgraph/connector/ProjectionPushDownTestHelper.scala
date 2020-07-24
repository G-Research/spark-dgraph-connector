package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.{Column, Dataset, Row}
import org.scalatest.Assertions
import uk.co.gresearch.spark.dgraph.connector.Predicate.columnNameForPredicateName
import uk.co.gresearch.spark.dgraph.connector.partitioner.PredicatePartitioner

trait ProjectionPushDownTestHelper extends Assertions {

  /**
   * Tests projection push down. An empty selection is interpreted as no selection.
   *
   * @param ds dataset
   * @param selection selection
   * @param expectedProjection expected projection
   * @param expectedDs expected dataset
   * @tparam T type of dataset
   */
  def doTestProjectionPushDownDf[T](ds: Dataset[T],
                                    selection: Seq[Column],
                                    expectedProjection: Option[Seq[Predicate]],
                                    expectedDs: Set[T]): Unit = {
    val expectedOutput =
      expectedProjection.map(_.map(p => columnNameForPredicateName(p.predicateName)).toSet)
        .getOrElse(ds.columns.toSet)
    val projectedDs = if (selection.nonEmpty) ds.select(selection: _*) else ds
    val plan = projectedDs.queryExecution.optimizedPlan
    val relationNode = plan match {
      case Project(_, child) => child
      case _ => plan
    }
    assert(relationNode.isInstanceOf[DataSourceV2ScanRelation])

    val relation = relationNode.asInstanceOf[DataSourceV2ScanRelation]
    assert(relation.outputSet.map(_.name).toSet === expectedOutput)
    assert(relation.scan.isInstanceOf[TripleScan])

    val scan = relation.scan.asInstanceOf[TripleScan]
    assert(scan.partitioner.isInstanceOf[PredicatePartitioner])

    val partitioner = scan.partitioner.asInstanceOf[PredicatePartitioner]
    assert(partitioner.projection === expectedProjection)

    val actual = projectedDs.collect()
    assert(actual.toSet === expectedDs)
    assert(actual.length === expectedDs.size)
  }

  def select(idx: Int*)(row: Row): Row = Row(idx.map(row.get): _*)

}
