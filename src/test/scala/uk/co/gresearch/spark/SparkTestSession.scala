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

package uk.co.gresearch.spark

import org.apache.spark.sql.SparkSession

trait SparkTestSession {

  def cores: Int = 1

  val spark: SparkSession = {
    SparkSession
      .builder()
      .master(s"local[$cores]")
      .appName("spark test example")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.local.dir", ".")
      .config("spark.sql.sources.v2.bucketing.enabled", "true")
      .getOrCreate()
  }

  implicit val session: SparkSession = spark

}
