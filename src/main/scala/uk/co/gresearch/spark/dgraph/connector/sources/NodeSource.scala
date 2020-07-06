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

package uk.co.gresearch.spark.dgraph.connector.sources

import java.util

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.{TypedNodeEncoder, WideNodeEncoder}
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.NodeTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.PartitionerProvider
import scala.collection.JavaConverters._

class NodeSource() extends TableProviderBase
  with TargetsConfigParser with SchemaProvider
  with ClusterStateProvider with PartitionerProvider {

  /**
   * Sets the number of predicates per partition to max int when predicate partitioner is used
   * in conjunction with wide node mode. Otherwise wide nodes cannot be properly loaded.
   *
   * @param options original options
   * @return modified options
   */
  def adjustOptions(options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    if (getStringOption(NodesModeOption, options).contains(NodesModeWideOption) &&
      getStringOption(PartitionerOption, options).forall(_.startsWith(PredicatePartitionerOption))) {
      if (getIntOption(PredicatePartitionerPredicatesOption, options).exists(_ != PredicatePartitionerPredicatesDefault)) {
        println("WARN: predicate partitioner enforced to a single partition to support wide node source")
      }

      new CaseInsensitiveStringMap(
        (options.asScala.filterKeys(!_.equalsIgnoreCase(PredicatePartitionerPredicatesOption)) ++
          Map(PredicatePartitionerPredicatesOption -> Int.MaxValue.toString)
          ).asJava
      )
    } else {
      options
    }
  }

  override def shortName(): String = "dgraph-nodes"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val adjustedOptions = adjustOptions(options)
    val targets = getTargets(adjustedOptions)
    val schema = getSchema(targets).filter(_.typeName != "uid")
    getNodeMode(adjustedOptions) match {
      case Some(NodesModeTypedOption) => TypedNodeEncoder.schema()
      case Some(NodesModeWideOption) => WideNodeEncoder.schema(schema.predicateMap)
      case Some(mode) => throw new IllegalArgumentException(s"Unknown node mode: ${mode}")
      case None => TypedNodeEncoder.schema()
    }
  }

  def getNodeMode(options: CaseInsensitiveStringMap): Option[String] =
    getStringOption(NodesModeOption, options)

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val adjustedOptions = adjustOptions(options)

    val targets = getTargets(adjustedOptions)
    val schema = getSchema(targets).filter(_.typeName != "uid")
    val clusterState = getClusterState(targets)
    val partitioner = getPartitioner(schema, clusterState, adjustedOptions)
    val nodeMode = getNodeMode(adjustedOptions)
    val encoder = nodeMode match {
      case Some(NodesModeTypedOption) => TypedNodeEncoder(schema.predicateMap)
      case Some(NodesModeWideOption) => WideNodeEncoder(schema.predicateMap)
      case Some(mode) => throw new IllegalArgumentException(s"Unknown node mode: ${mode}")
      case None => TypedNodeEncoder(schema.predicateMap)
    }
    val execution = DgraphExecutorProvider()
    val chunkSize = getIntOption(ChunkSizeOption, adjustedOptions)
    val model = NodeTableModel(execution, encoder, chunkSize)
    new TripleTable(partitioner, model, clusterState.cid)
  }

}
