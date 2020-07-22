/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel

/**
 * Partition of Dgraph data. Reads all triples with the given predicates in the given uid range.
 * Providing object values will return only those triples that match the predicate name and value.
 *
 * @param targets Dgraph alpha nodes
 * @param operators set of operators
 * @param model table model
 */
case class Partition(targets: Seq[Target], operators: Set[Operator] = Set.empty, model: GraphTableModel)
  extends InputPartition[InternalRow] {

  def has(predicates: Set[Predicate]): Partition =
    copy(operators = operators ++ Set(Has(predicates)))

  def has(properties: Set[String], edges: Set[String]): Partition =
    copy(operators = operators ++ Set(Has(properties, edges)))

  def get(predicates: Set[Predicate]): Partition =
    copy(operators = operators ++ Set(Get(predicates)))

  def getAll(): Partition =
    copy(operators = operators ++ operators.filter(_.isInstanceOf[Has]).map(_.asInstanceOf[Has]).map(has => Get(has.properties, has.edges)))

  def eq(predicate: String, values: Set[Any]): Partition =
    copy(operators = operators ++ Set(IsIn(predicate, values)))

  def eq(predicates: Set[String], values: Set[Any]): Partition =
    copy(operators = operators ++ Set(IsIn(predicates, values)))

  val uids: Option[UidRange] =
    Some(operators.filter(_.isInstanceOf[UidRange]).map(_.asInstanceOf[UidRange]))
      .filter(_.nonEmpty)
      .map(_.head)

  val predicates: Set[String] =
    operators.flatMap {
      case op: Get => Some(op.properties ++ op.edges)
      case _ => None
    }.headOption.getOrElse(Set.empty)

  // TODO: use host names of Dgraph alphas to co-locate partitions
  override def preferredLocations(): Array[String] = super.preferredLocations()

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new TriplePartitionReader(this, model)

  /**
   * Provide the query representing this partitions sub-graph.
   * @return partition query
   */
  def query: PartitionQuery = PartitionQuery.of(this)

}
