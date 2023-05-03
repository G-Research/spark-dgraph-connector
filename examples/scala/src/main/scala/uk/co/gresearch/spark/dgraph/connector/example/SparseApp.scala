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

package uk.co.gresearch.spark.dgraph.connector.example

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.scalactic.TripleEquals
import org.graphframes.GraphFrame
import uk.co.gresearch.spark.dgraph.connector.{IncludeReservedPredicatesOption, TypedNode}
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.graphx.{EdgeProperty, VertexProperty}

object SparseApp {

  def main(args: Array[String]): Unit = {
    import TripleEquals._

    val spark: SparkSession = {
      SparkSession
        .builder()
        .master(s"local[*]")
        .appName("spark dgraph example")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.local.dir", ".")
        .getOrCreate()
    }
    import spark.implicits._

    val target = "localhost:9080"

    val df = spark.read
      .option("dgraph.partitioner.predicate.predicatesPerPartition", 1)
      .dgraph.triples(target)
      .cache

    df.select($"subject")
      .distinct
      .groupBy((($"subject" / 1000000000000000000L).cast("long")*1000000000000000000L).as("id-group"))
      .count
      .orderBy($"id-group")
      .show

    Console.println(s"${df.count()} rows")

    assert(df.count() === 1041295)
  }

}
