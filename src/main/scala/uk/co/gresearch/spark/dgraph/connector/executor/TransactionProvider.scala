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

package uk.co.gresearch.spark.dgraph.connector.executor

import io.grpc.ManagedChannel
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.{ConfigParser, Target, Transaction, TransactionModeDefault, TransactionModeNoneOption, TransactionModeOption, TransactionModeReadOption, getClientFromChannel, toChannel}

trait TransactionProvider extends ConfigParser {

  def getTransaction(targets: Seq[Target], options: CaseInsensitiveStringMap): Option[Transaction] = {
    getStringOption(TransactionModeOption, options, TransactionModeDefault) match {
      case TransactionModeNoneOption => None
      case TransactionModeReadOption => Some(getTransaction(targets))
      case mode => throw new IllegalArgumentException(s"Unsupported transaction mode: $mode")
    }
  }

  private def getTransaction(targets: Seq[Target]): Transaction = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val client = getClientFromChannel(channels)
      val transaction = client.newReadOnlyTransaction()
      val response = transaction.query("{ result (func: uid(0x0)) { } }")
      Transaction(response.getTxn)
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
