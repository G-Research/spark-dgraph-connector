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

/**
 * Operator for PartitionQuery.
 */
trait Operator

/**
 * Adds @lang directive to the set of predicates. This operator provides schema
 * information to the PartitionQuery class but does not invoke any operation itself.
 * @param predicates predicates that have lang directive
 */
case class LangDirective(predicates: Set[String]) extends Operator

/**
 * Operator on a single predicate.
 */
trait PredicateOperator extends Operator {
  val filter: String
  val predicates: Set[String]
}

/**
 * Operator on a single predicate with a single value.
 */
trait PredicateValueOperator extends PredicateOperator {
  val value: Any
}

/**
 * Operator on a single predicate with a set of values.
 */
trait PredicateValuesOperator extends PredicateOperator {
  val values: Set[Any]
}

/**
 * Uids of the result match any of the given uids.
 * @param uids uids
 */
case class Uids(uids: Set[Uid]) extends Operator

/**
 * Uids of the result in a range of uids.
 * @param first first uid of range (inclusive)
 * @param until last uid of range (exclusive)
 */
case class UidRange(first: Uid, until: Uid) extends Operator {
  if (first >= until)
    throw new IllegalArgumentException(s"UidRange first uid (is $first) must be before until (is $until)")
  def length: Long = until.uid - first.uid
}

/**
 * The uids in the result have any of the given properties or edges.
 * @param properties predicate names with non-uid values
 * @param edges predicate names with uid values
 */
case class Has(properties: Set[String], edges: Set[String]) extends Operator
object Has {
  def apply(predicates: Set[Predicate]): Has = {
    val props = predicates.filter(_.isProperty).map(_.predicateName)
    val edges = predicates.filter(_.isEdge).map(_.predicateName)
    Has(props, edges)
  }
}

/**
 * The properties or edges to retrieve.
 * @param properties predicate names with non-uid values
 * @param edges predicate names with uid values
 */
case class Get(properties: Set[String], edges: Set[String]) extends Operator
object Get {
  def apply(predicates: Set[Predicate]): Get = {
    val props = predicates.filter(_.isProperty).map(_.predicateName)
    val edges = predicates.filter(_.isEdge).map(_.predicateName)
    Get(props, edges)
  }
}

/**
 * The given predicate has any of the given values for uids in the result.
 * @param predicates predicate names
 * @param values values
 */
case class IsIn(predicates: Set[String], values: Set[Any]) extends PredicateValuesOperator {
  val filter = "eq"
}
object IsIn {
  def apply(predicate: String, values: Set[Any]): IsIn = IsIn(Set(predicate), values)
}

/**
 * The given predicate has a value that is less than the given value for uids in the result.
 * @param predicates predicate names
 * @param value value
 */
case class LessThan(predicates: Set[String], value: Any) extends PredicateValueOperator {
  val filter = "lt"
}
object LessThan {
  def apply(predicate: String, value: Any): LessThan = LessThan(Set(predicate), value)
}

/**
 * The given predicate has a value that is less than or equal to the given value for uids in the result.
 * @param predicates predicate names
 * @param value value
 */
case class LessOrEqual(predicates: Set[String], value: Any) extends PredicateValueOperator {
  val filter = "le"
}
object LessOrEqual {
  def apply(predicate: String, value: Any): LessOrEqual = LessOrEqual(Set(predicate), value)
}

/**
 * The given predicate has a value that is greater than the given value for uids in the result.
 * @param predicates predicate names
 * @param value value
 */
case class GreaterThan(predicates: Set[String], value: Any) extends PredicateValueOperator {
  val filter = "gt"
}
object GreaterThan {
  def apply(predicate: String, value: Any): GreaterThan = GreaterThan(Set(predicate), value)
}

/**
 * The given predicate has a value that is greater than or equal to the given value for uids in the result.
 * @param predicates predicate names
 * @param value value
 */
case class GreaterOrEqual(predicates: Set[String], value: Any) extends PredicateValueOperator {
  val filter = "ge"
}
object GreaterOrEqual {
  def apply(predicate: String, value: Any): GreaterOrEqual = GreaterOrEqual(Set(predicate), value)
}
