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

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, In, Not}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.{Column, Dataset}
import org.scalatest.Assertions
import uk.co.gresearch.spark.dgraph.connector.partitioner.PredicatePartitioner

trait FilterPushdownTestHelper extends Assertions {

  // we filter out some dgraph nodes to get consistent test data
  // we remove those filters here
  def removedSubjectNotInFilter(expressions: Seq[Expression]): Seq[Expression] =
    expressions.filter {
      case Not(In(ref, _)) if ref.isInstanceOf[AttributeReference] && ref.asInstanceOf[AttributeReference].name == "subject" => false
      case Not(EqualTo(ref, _)) if ref.isInstanceOf[AttributeReference] && ref.asInstanceOf[AttributeReference].name == "subject" => false
      case _ => true
    }

  def doTestFilterPushDownDf[T](ds: Dataset[T],
                                condition: Column,
                                expectedFilters: Set[Filter],
                                expectedUnpushed: Seq[Expression] = Seq.empty,
                                expecteds: Set[T] = Set.empty): Unit = {
    val conditionedDs = ds.where(condition)
    val plan = conditionedDs.queryExecution.optimizedPlan
    val relationNode = plan match {
      case filter: logical.Filter =>
        val unpushedFilters = removedSubjectNotInFilter(getFilterNodes(filter.condition))
        assert(unpushedFilters.map(_.sql).sorted === expectedUnpushed.map(_.sql).sorted)
        filter.child
      case _ =>
        assert(expectedUnpushed.isEmpty, "some unpushed filters expected but none filters actually unpushed")
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
    assert(actual.toSet === expecteds)
    assert(actual.length === expecteds.size)
  }

  def getFilterNodes(node: Expression): Seq[Expression] = node match {
    case And(left, right) => getFilterNodes(left) ++ getFilterNodes(right)
    case _ => Seq(node)
  }

}
