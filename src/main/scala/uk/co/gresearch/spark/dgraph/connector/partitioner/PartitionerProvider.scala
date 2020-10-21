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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Schema, Transaction}

trait PartitionerProvider {

  val partitionerOptions = Seq(
    new ConfigPartitionerOption(),
    new DefaultPartitionerOption()
  )

  def getPartitioner(schema: Schema,
                     clusterState: ClusterState,
                     transaction: Transaction,
                     options: CaseInsensitiveStringMap): Partitioner =
    partitionerOptions
      .flatMap(_.getPartitioner(schema, clusterState, transaction, options))
      .headOption
      .getOrElse(throw new RuntimeException("Could not find any suitable partitioner"))

}
