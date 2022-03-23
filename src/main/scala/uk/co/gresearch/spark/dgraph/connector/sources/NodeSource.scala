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

package uk.co.gresearch.spark.dgraph.connector.sources

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.{TypedNodeEncoder, WideNodeEncoder}
import uk.co.gresearch.spark.dgraph.connector.executor.{DgraphExecutorProvider, TransactionProvider}
import uk.co.gresearch.spark.dgraph.connector.model.NodeTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.{PartitionerOption, PartitionerProvider, PartitionerProviderOption}

import java.util

class NodeSource() extends TableProviderBase
  with TargetsConfigParser with SchemaProvider
  with ClusterStateProvider with PartitionerProvider
  with TransactionProvider with Logging {

  override def shortName(): String = "dgraph-nodes"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val targets = getTargets(options)
    val schema = getSchema(targets, options).filter(_.isProperty)
    getNodeMode(options) match {
      case Some(NodesModeTypedOption) => TypedNodeEncoder.schema
      case Some(NodesModeWideOption) => WideNodeEncoder.schema(schema.predicateMap)
      case Some(mode) => throw new IllegalArgumentException(s"Unknown node mode: ${mode}")
      case None => TypedNodeEncoder.schema
    }
  }

  def getNodeMode(options: CaseInsensitiveStringMap): Option[String] =
    getStringOption(NodesModeOption, options)

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val targets = getTargets(options)
    val transaction = getTransaction(targets, options)
    val execution = DgraphExecutorProvider(transaction)
    val schema = getSchema(targets, options).filter(_.isProperty)
    val clusterState = getClusterState(targets, options)
    val nodeMode = getNodeMode(options)
    val encoder = nodeMode match {
      case Some(NodesModeTypedOption) => TypedNodeEncoder(schema.predicateMap)
      case Some(NodesModeWideOption) => WideNodeEncoder(schema.predicates)
      case Some(mode) => throw new IllegalArgumentException(s"Unknown node mode: ${mode}")
      case None => TypedNodeEncoder(schema.predicateMap)
    }
    val defaultPartitionerProvider = nodeMode.filter(_.equals(NodesModeWideOption))
      .map(_ => NodeSource.wideNodeDefaultPartitionProvider)
      .getOrElse(PartitionerProvider.defaultPartitionProvider)
    val partitioner = getPartitioner(schema, clusterState, transaction, options, defaultPartitionerProvider)
    if (nodeMode.exists(_.equals(NodesModeWideOption)) &&
      partitioner.getPartitionColumns.exists(_.contains("predicate"))) {
      throw new RuntimeException(s"Dgraph nodes cannot be read in wide mode " +
        s"with any kind of predicate partitioning: ${partitioner.configOption}")
    }
    val chunkSize = getIntOption(ChunkSizeOption, options, ChunkSizeDefault)
    val model = NodeTableModel(execution, encoder, chunkSize)
    TripleTable(partitioner, model, clusterState.cid)
  }

}

object NodeSource {
  val wideNodeDefaultPartitionProvider: PartitionerProviderOption = new PartitionerOption(SingletonPartitionerOption)
}
