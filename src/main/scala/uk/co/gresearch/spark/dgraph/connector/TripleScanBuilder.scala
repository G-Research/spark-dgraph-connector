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

import org.apache.spark.sql
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters}
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.Partitioner

import scala.collection.mutable

case class TripleScanBuilder(partitioner: Partitioner, model: GraphTableModel) extends ScanBuilder
  with SupportsPushDownFilters {

  val pushed: mutable.Set[sql.sources.Filter] = mutable.Set.empty
  var filters: Filters = EmptyFilters
  val translator: FilterTranslator = FilterTranslator(model.encoder)

  // We push filters into the Dgraph queries as follows:
  // 1) the FilterTranslator translates them into connector-specific filters
  //    for this the translator needs the encoder to know which column name refers to what kind of column
  //    we have the subject and predicate name column, predicate values columns, object values columns and object type column (see README.md)
  // 2) the actual partitioning uses the filters to create according partitions
  //    hence the partitioner tells us here if specific filters are supported
  // Only supported filters are promised to Spark to be applied (we then have to apply them otherwise Spark reads incorrect data).
  // All filters translated from a Spark filter need to be supported in order to promise that filter to Spark.
  // All unsupported filters are still passed to the partitioner as optional.
  // Filters(promised, optional) is the structure to hold those two set of filters.
  // The FilterTranslator implements some simplification logic of a Seq[Filter].
  override def pushFilters(filters: Array[sql.sources.Filter]): Array[sql.sources.Filter] = {
    println(s"pushing filters: ${filters.mkString(", ")}")
    val translated = filters.map(f => f -> translator.translate(f)).toMap
    val (supported, unsupported) = translated.partition(t => t._2.exists(partitioner.supportsFilters))
    val translatedFilters = Filters(supported.values.flatten.flatten.toSeq, unsupported.values.flatten.flatten.toSeq)
    val simplifiedFilters = FilterTranslator.simplify(translatedFilters, partitioner.supportsFilters)
    println(s"promised filters: ${supported.mapValues(_.get).mkString(", ")}")
    println(s"unsupported filters: ${unsupported.keys.mkString(", ")}")
    println(s"pushed filters: ${translated.filter(_._2.isDefined).keys.mkString(", ")}")
    println(s"applied filters: ${simplifiedFilters.mkString(", ")}")
    this.pushed ++= translated.filter(_._2.isDefined).keys
    this.filters = simplifiedFilters
    unsupported.keys.toArray
  }

  override def pushedFilters(): Array[sql.sources.Filter] = pushed.clone().toArray

  override def build(): Scan = TripleScan(partitioner.withFilters(filters), model)

}
