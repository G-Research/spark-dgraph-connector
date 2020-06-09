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

import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedNodeEncoder
import uk.co.gresearch.spark.dgraph.connector.model.NodeTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.PartitionerProvider
import uk.co.gresearch.spark.dgraph.connector.{ClusterStateProvider, SchemaProvider, TableProviderBase, TargetsConfigParser, TripleScan}

class NodeSource() extends TableProviderBase
  with TargetsConfigParser with SchemaProvider
  with ClusterStateProvider with PartitionerProvider {

  override def shortName(): String = "dgraph-nodes"

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val targets = getTargets(options)
    val schema = getSchema(targets).filter(_.typeName != "uid")
    val clusterState = getClusterState(targets)
    val partitioner = getPartitioner(schema, clusterState, options)
    val encoder = TypedNodeEncoder(schema.predicateMap)
    val model = NodeTableModel(encoder)
    new TripleScan(partitioner, model)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader =
    super.createReader(schema, options)

}
