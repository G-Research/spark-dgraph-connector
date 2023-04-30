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
import io.dgraph.dgraph4j.shaded.io.grpc.ManagedChannel
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util.regex.Pattern

import scala.collection.JavaConverters._

trait SchemaProvider {

  private val query = "schema { predicate type lang }"

  def getSchema(targets: Seq[Target], options: CaseInsensitiveStringMap): Schema = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    val reservedPredicateFilter = ReservedPredicateFilter(options)

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
        .filter(p => reservedPredicateFilter.apply(p.predicateName))
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
        throw new RuntimeException(
          s"Failed to retrieve the schema from one of the targets: ${targets.mkString(", ")}", exc
        )
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
