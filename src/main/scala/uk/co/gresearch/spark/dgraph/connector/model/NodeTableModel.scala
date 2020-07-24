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

package uk.co.gresearch.spark.dgraph.connector.model

import uk.co.gresearch.spark.dgraph.connector.encoder.JsonNodeInternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.{NoPartitionMetrics, PartitionMetrics}

/**
 * Models only the nodes of a graph as a table.
 */
case class NodeTableModel(execution: ExecutorProvider,
                          encoder: JsonNodeInternalRowEncoder,
                          chunkSize: Int,
                          metrics: PartitionMetrics = NoPartitionMetrics())
  extends GraphTableModel {

  override def withMetrics(metrics: PartitionMetrics): NodeTableModel = copy(metrics = metrics)

  override def withEncoder(encoder: JsonNodeInternalRowEncoder): GraphTableModel = copy(encoder = encoder)

}
