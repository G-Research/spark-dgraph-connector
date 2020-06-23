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
import uk.co.gresearch.spark.dgraph.connector.encoder.{StringTripleEncoder, TypedTripleEncoder}
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.TripleTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.PartitionerProvider

class TripleSource() extends TableProviderBase
  with TargetsConfigParser with SchemaProvider
  with ClusterStateProvider with PartitionerProvider {

  override def shortName(): String = "dgraph-triples"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    getTripleMode(options) match {
      case Some(TriplesModeStringOption) => StringTripleEncoder.schema()
      case Some(TriplesModeTypedOption) => TypedTripleEncoder.schema()
      case Some(mode) => throw new IllegalArgumentException(s"Unknown triple mode: ${mode}")
      case None => TypedTripleEncoder.schema()
    }

  def getTripleMode(options: CaseInsensitiveStringMap): Option[String] =
    getStringOption(TriplesModeOption, options)

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val targets = getTargets(options)
    val schema = getSchema(targets)
    val clusterState = getClusterState(targets)
    val partitioner = getPartitioner(schema, clusterState, options)
    val tripleMode = getTripleMode(options)
    val encoder = tripleMode match {
      case Some(TriplesModeStringOption) => StringTripleEncoder(schema.predicateMap)
      case Some(TriplesModeTypedOption) => TypedTripleEncoder(schema.predicateMap)
      case Some(mode) => throw new IllegalArgumentException(s"Unknown triple mode: ${mode}")
      case None => TypedTripleEncoder(schema.predicateMap)
    }
    val execution = DgraphExecutorProvider()
    val model = TripleTableModel(execution, encoder)
    new TripleTable(partitioner, model, clusterState.cid)
  }

}
