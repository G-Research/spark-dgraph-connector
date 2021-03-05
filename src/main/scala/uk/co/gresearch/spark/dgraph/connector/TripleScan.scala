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

import org.apache.spark.sql
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.encoder.ProjectedSchema
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.Partitioner

import scala.collection.JavaConverters._
import scala.collection.mutable

case class TripleScan(partitioner: Partitioner, model: GraphTableModel)
  extends DataSourceReader
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with Logging {

  var requiredSchema: Option[StructType] = None
  var appliedPartitioner: Partitioner = partitioner

  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (log.isDebugEnabled) {
      val columns = abbreviate(requiredSchema.fields.toSeq.map(_.name).mkString(", "))
      log.debug(s"required columns: $columns")
    }
    this.requiredSchema = Some(requiredSchema)

    // get optional projection (projected predicates) from model's encoder
    val projection: Option[Seq[Predicate]] =
      Some(modelWithOptionalSchema.encoder)
        .filter(_.isInstanceOf[ProjectedSchema])
        .flatMap(_.asInstanceOf[ProjectedSchema].readPredicates)

    // apply optional projection to partitioner
    appliedPartitioner = projection.foldLeft(partitioner)((part, proj) => part.withProjection(proj))
  }

  // apply optional schema to model
  def modelWithOptionalSchema: GraphTableModel = requiredSchema.fold(model)(model.withSchema)

  override def readSchema(): StructType = modelWithOptionalSchema.readSchema()

  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] =
    appliedPartitioner
      .withFilters(filters)
      .getPartitions(modelWithOptionalSchema)
      .map(_.asInstanceOf[InputPartition[InternalRow]])
      .toList.asJava

  val pushed: mutable.Set[sql.sources.Filter] = mutable.Set.empty
  var filters: Filters = EmptyFilters
  val translator: FilterTranslator = FilterTranslator(model.encoder)  // modelWithOptionalSchema not needed here

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
    val translated = filters.map(f => f -> translator.translate(f)).toMap
    val (supported, unsupported) = translated.partition(t => t._2.exists(partitioner.supportsFilters))
    val translatedFilters = Filters(supported.values.flatten.flatten.toSet, unsupported.values.flatten.flatten.toSet)
    val simplifiedFilters = FilterTranslator.simplify(translatedFilters, partitioner.supportsFilters)
    this.pushed ++= translated.filter(_._2.isDefined).keys
    this.filters = simplifiedFilters

    if (filters.nonEmpty) log.debug(s"pushing filters: ${filters.mkString(", ")}")
    if (supported.nonEmpty) log.trace(s"promised filters: ${supported.mapValues(_.get).mkString(", ")}")
    if (unsupported.nonEmpty) log.debug(s"unsupported filters: ${unsupported.keys.mkString(", ")}")
    // only report pushed filters if filters are given
    if (filters.nonEmpty) log.debug(s"pushed filters: ${translated.filter(_._2.isDefined).keys.mkString(", ")}")
    if (simplifiedFilters.nonEmpty) log.trace(s"applied filters: ${simplifiedFilters.mkString(", ")}")

    unsupported.keys.toArray
  }

  override def pushedFilters(): Array[sql.sources.Filter] = pushed.clone().toArray

}
