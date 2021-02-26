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
import io.grpc.ManagedChannel
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util.regex.Pattern

import scala.collection.JavaConverters._

trait SchemaProvider extends ConfigParser {

  private val query = "schema { predicate type lang }"

  def getPredicateFilters(filters: String): Set[Pattern] = {
    val replacements = Seq(
      (".", "\\."),
      ("?", "\\?"),
      ("[", "\\["), ("]", "\\]"),
      ("{", "\\{"), ("}", "\\}"),
      ("*", ".*"),
    )

    val filterStrings = filters.split(",")
    if (filterStrings.exists(!_.startsWith("dgraph."))) {
      throw new IllegalArgumentException(s"Reserved predicate filters must start with 'dgraph.': ${filterStrings.mkString(", ")}")
    }

    filterStrings
      .map(f => replacements.foldLeft[String](f) { case (f, (pat, repl)) => f.replace(pat, repl) })
      .map(Pattern.compile)
      .toSet
  }

  def getSchema(targets: Seq[Target], options: CaseInsensitiveStringMap): Schema = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    val includes = getStringOption(IncludeReservedPredicatesOption, options)
      .orElse(Some("dgraph.*"))
      .map(getPredicateFilters)
      .get
    val excludes = getStringOption(ExcludeReservedPredicatesOption, options)
      .map(getPredicateFilters)
      .getOrElse(Set())

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
        .filter(p =>
          ! p.predicateName.startsWith("dgraph.") ||
          includes.exists(_.matcher(p.predicateName).matches()) &&
          ! excludes.exists(_.matcher(p.predicateName).matches()))
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
