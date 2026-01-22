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

import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.Partitioner

import java.util
import java.util.UUID
import scala.jdk.CollectionConverters._

case class TripleTable(partitioner: Partitioner, model: GraphTableModel, cid: UUID)
  extends TableBase with SupportsWrite {

  override def schema(): StructType = model.schema()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    TripleScanBuilder(partitioner, model)

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder =
    TripleWriteBuilder(logicalWriteInfo.schema(), model)

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.ACCEPT_ANY_SCHEMA
  ).asJava

}
