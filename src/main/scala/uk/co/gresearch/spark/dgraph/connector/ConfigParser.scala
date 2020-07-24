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

import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait ConfigParser {

  def getStringOption(option: String, options: CaseInsensitiveStringMap): Option[String] =
    Option(options.get(option))

  def getStringOption(option: String, options: CaseInsensitiveStringMap, default: String): String =
    getStringOption(option, options).getOrElse(default)

  def getIntOption(option: String, options: CaseInsensitiveStringMap): Option[Int] =
    Option(options.get(option)).map(_.toInt)

  def getIntOption(option: String, options: CaseInsensitiveStringMap, default: Int): Int =
    getIntOption(option, options).getOrElse(default)

}
