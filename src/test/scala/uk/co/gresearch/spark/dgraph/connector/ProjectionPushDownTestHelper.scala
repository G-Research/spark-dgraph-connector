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

package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, In, Not}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.{Column, Dataset, Row}
import org.scalatest.Assertions
import uk.co.gresearch.spark.dgraph.connector.Predicate.columnNameForPredicateName
import uk.co.gresearch.spark.dgraph.connector.partitioner.PredicatePartitioner

trait ProjectionPushDownTestHelper extends Assertions {

  def select(idx: Int*)(row: Row): Row = Row(idx.map(row.get): _*)

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
    val unprojectedPlan = plan match {
      case Project(_, child) => child
      case _ => plan
    }
    val unfilteredPlan = unprojectedPlan match {
      // some subjects might be excluded here as they refer to dgraph reserved nodes
      case logical.Filter(Not(In(ref, _)), child) if ref.isInstanceOf[AttributeReference] && ref.asInstanceOf[AttributeReference].name == "subject" => child
      case logical.Filter(Not(EqualTo(ref, _)), child) if ref.isInstanceOf[AttributeReference] && ref.asInstanceOf[AttributeReference].name == "subject" => child
      case _ => unprojectedPlan
    }
    val relationNode = unfilteredPlan match {
      case Project(_, child) => child
      case _ => unfilteredPlan
    }
    assert(relationNode.isInstanceOf[DataSourceV2ScanRelation])

    val relation = relationNode.asInstanceOf[DataSourceV2ScanRelation]
    val dsOutput = projectedDs.queryExecution.optimizedPlan.outputSet.map(_.name).toSet
    val actualOutput = {
      val output = relation.outputSet.map(_.name).toSet
      // we expect there to be a "subject" in the output when we have filtered for it
      if (unfilteredPlan != unprojectedPlan && !dsOutput.contains("subject")) output - "subject" else output
    }
    assert(actualOutput === expectedOutput)
    assert(relation.scan.isInstanceOf[TripleScan])

    val scan = relation.scan.asInstanceOf[TripleScan]
    assert(scan.partitioner.isInstanceOf[PredicatePartitioner])

    val partitioner = scan.partitioner.asInstanceOf[PredicatePartitioner]
    val actualProjection = {
      val projection = partitioner.projection
      // we expect there to be a "subject" in the projection when we have filtered for it
      if (unfilteredPlan != unprojectedPlan && !dsOutput.contains("subject")) projection.map(_.filterNot(p => p.predicateName == "uid" && p.dgraphType == "subject")) else projection
    }
    assert(actualProjection === expectedProjection)

    val actual = projectedDs.collect()
    assert(actual.toSet === expectedDs)
    assert(actual.length === expectedDs.size)
  }

}
