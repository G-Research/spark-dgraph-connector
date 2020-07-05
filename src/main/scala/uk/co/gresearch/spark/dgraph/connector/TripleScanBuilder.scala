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
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.Partitioner

import scala.collection.mutable

case class TripleScanBuilder(partitioner: Partitioner, model: GraphTableModel) extends ScanBuilder
  with SupportsPushDownFilters {

  val pushed: mutable.Set[sql.sources.Filter] = mutable.Set.empty
  val filters: mutable.Set[connector.Filter] = mutable.Set.empty
  val translator: FilterTranslator = FilterTranslator(model.encoder)

  override def pushFilters(filters: Array[sql.sources.Filter]): Array[sql.sources.Filter] = {
    println(s"pushing filters: ${filters.mkString(", ")}")
    val translated = filters.map(f => f -> translator.translate(f)).toMap
    val (pushed, unsupported) = translated.partition(t => t._2.exists(f => f.forall(partitioner.supportsFilter)))
    println(s"pushed filters: ${pushed.mapValues(_.get).mkString(", ")}")
    println(s"unsupported filters: ${unsupported.keys.mkString(", ")}")
    println(s"applied filters: ${translated.values.flatten.flatten.mkString(", ")}")
    this.pushed ++= translated.filter(_._2.isDefined).keys
    this.filters ++= translated.values.flatten.flatten
    unsupported.keys.toArray
  }

  override def pushedFilters(): Array[sql.sources.Filter] = pushed.clone().toArray

  override def build(): Scan = TripleScan(partitioner.withFilters(filters.toSeq), model)

}