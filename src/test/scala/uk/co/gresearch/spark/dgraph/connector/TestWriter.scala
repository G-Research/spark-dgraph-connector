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
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.DgraphTestCluster

class TestWriter extends AnyFunSpec with ConnectorSparkTestSession with DgraphTestCluster {

  import spark.implicits._

  // we want a fresh cluster that we can mutate, definitively not one that is always running and used by all tests
  override val clusterAlwaysStartUp: Boolean = true

  describe("Connector") {
    it("should write") {
      spark.range(0, 1000000, 1, 10)
        .select(
          $"id".as("subject"),
          $"id".cast(StringType).as("str"),
          $"id".cast(DoubleType).as("dbl")
        )
        .repartition($"subject")
        .sortWithinPartitions($"subject")
        .write
        .mode(SaveMode.Overwrite)
        .option("dgraph.nodes.mode", "wide")
        .format("uk.co.gresearch.spark.dgraph.nodes")
        .save(dgraph.target)
    }
  }
}