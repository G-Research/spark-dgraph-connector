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

import com.google.gson.{Gson, JsonObject}
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.{ManagedChannel, Status, StatusRuntimeException}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

trait SchemaProvider {

  private val query = "schema { predicate type lang }"

  def getSchema(targets: Seq[Target]): Schema = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val client: DgraphClient = getClientFromChannel(channels)
      val response: Response = client.newReadOnlyTransaction().query(query)
      val json: String = response.getJson.toStringUtf8
      val schema = new Gson().fromJson(json, classOf[JsonObject])
        .get("schema").getAsJsonArray.asScala
        .map(_.getAsJsonObject)
        .map(o => Predicate(
          o.get("predicate").getAsString,
          o.get("type").getAsString,
          o.has("lang") && o.get("lang").getAsBoolean
        ))
        .toSet
      Schema(schema)
    } catch {
      case e: Throwable =>
        // this is potentially an async exception which does not include any useful stacktrace, so we add it here
        val exc = e.fillInStackTrace()

        // provide a useful exception when we see an RESOURCE_EXHAUSTED gRPC code
        if (exc.causedByResourceExhausted())
          throw new RuntimeException(
            "Schema could not be retrieved, because it is too large. " +
              "Increasing the maximum size is not supported: " +
              "https://github.com/G-Research/spark-dgraph-connector/issues/71",
            exc
          )

        // just rethrow any other exception
        throw exc
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
