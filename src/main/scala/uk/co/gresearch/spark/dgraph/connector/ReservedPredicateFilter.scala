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

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.ReservedPredicateFilter.getPredicateFilters

import java.util.regex.Pattern

case class ReservedPredicateFilter(options: CaseInsensitiveStringMap) extends ConfigParser {

  val includes: Set[Pattern] = getStringOption(IncludeReservedPredicatesOption, options)
    .orElse(Some("dgraph.*"))
    .map(getPredicateFilters)
    .get
  val excludes: Set[Pattern] = getStringOption(ExcludeReservedPredicatesOption, options)
    .map(getPredicateFilters)
    .getOrElse(Set())

  def apply(predicateName: String): Boolean =
    ! predicateName.startsWith("dgraph.") ||
      includes.exists(_.matcher(predicateName).matches()) &&
        excludes.forall(!_.matcher(predicateName).matches())

}

object ReservedPredicateFilter {

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

}