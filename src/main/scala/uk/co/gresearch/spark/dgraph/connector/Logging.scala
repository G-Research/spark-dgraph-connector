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

import java.text.NumberFormat

import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{LogManager, Logger}

trait Logging {

  val loggingStringMaxLength = 1000
  val loggingStringAbbreviateMiddle = "[…]"

  lazy val loggingFormat: NumberFormat = NumberFormat.getInstance()

  @transient
  lazy val log: Logger = LogManager.getLogger(this.getClass)

  def abbreviate(string: String): String =
    StringUtils.abbreviateMiddle(string, loggingStringAbbreviateMiddle, loggingStringMaxLength)

}
