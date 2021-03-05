/*
 * Copyright 2021 G-Research
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

import org.apache.spark.sql.DataFrameReader
import uk.co.gresearch.spark.SparkTestSession

trait ConnectorSparkTestSession extends SparkTestSession {

  // Dgraph tends to add new reserved predicates across versions
  // which results in varying schemata, so we exclude all except the type information
  // to get stable test data
  def reader: DataFrameReader = spark.read.option(IncludeReservedPredicatesOption, "dgraph.type")

}
