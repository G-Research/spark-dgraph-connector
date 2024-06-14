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

import org.apache.spark.sql.SaveMode
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.SparkTestSession

class TestDbpedia extends AnyFunSpec with SparkTestSession {

  import spark.implicits._

  override def cores: Int = 1

  describe("Dbpedia") {
    ignore("should read everything") {
      val df =
        spark.read
          .options(
            Map(
              ChunkSizeOption -> "100000",
              PartitionerOption -> s"$PredicatePartitionerOption+$UidRangePartitionerOption",
              PredicatePartitionerPredicatesOption -> "10",
              UidRangePartitionerUidsPerPartOption -> "10000000",
              NodesModeOption -> NodesModeWideOption
            )
          )
//          .dgraph.triples("localhost:9080")
//          .where($"predicate" === "http://de.dbpedia.org/property/kurzbeschreibung")
//          .where($"predicate" === "http://www.w3.org/2000/01/rdf-schema#label")
          .dgraph
          .nodes("localhost:9080")

      df
        .select("`http://www.w3.org/2000/01/rdf-schema#label`")
        .where($"`http://www.w3.org/2000/01/rdf-schema#label`".isNotNull)
        .write
        .mode(SaveMode.Overwrite)
        .parquet("/tmp/dgraph.parquet")

//      Console.readLine()
    }
  }
}
