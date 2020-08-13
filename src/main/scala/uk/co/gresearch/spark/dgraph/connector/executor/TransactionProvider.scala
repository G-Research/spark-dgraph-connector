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
import uk.co.gresearch.spark.dgraph.connector.{Target, Transaction, getClientFromChannel, toChannel}

trait TransactionProvider {
  def getTransaction(targets: Seq[Target]): Transaction = {
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
