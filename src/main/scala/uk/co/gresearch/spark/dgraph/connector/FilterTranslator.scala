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

import org.apache.spark.sql
import org.apache.spark.sql.sources._
import uk.co.gresearch.spark.dgraph.connector.encoder.ColumnInfo

/**
 * A column name is enclosed with backticks when it contains dots (.). This unapply is used to
 * remove those backticks during pattern matching if they exist.
 */
object ColumnName {
  def unapply(columnName: String): Option[String] =
    if (columnName.startsWith("`") && columnName.endsWith("`"))
      Some(columnName.drop(1).dropRight(1))
    else
      Some(columnName)
}

case class FilterTranslator(columnInfo: ColumnInfo) {

  /**
   * Translates the spark filter to some dgraph connector filters, or None if no translation available.
   * @param filter spark filter
   * @return Some dgraph filters
   */
  def translate(filter: sql.sources.Filter): Option[Set[Filter]] = filter match {
    case IsNotNull(ColumnName(column)) if columnInfo.isPredicateValueColumn(column) =>
      Some(Set(PredicateNameIs(column)))
    case IsNotNull(ColumnName(column)) if columnInfo.isObjectValueColumn(column) && columnInfo.getObjectType(column).isDefined =>
      Some(Set(ObjectTypeIsIn(columnInfo.getObjectType(column).get)))

    case EqualTo(ColumnName(column), value) if columnInfo.isSubjectColumn(column) && Option(value).isDefined =>
      Some(Set(SubjectIsIn(Uid(value.toLong))))
    case EqualTo(ColumnName(column), value) if columnInfo.isPredicateColumn(column) && Option(value).isDefined =>
      Some(Set(IntersectPredicateNameIsIn(value.toString)))
    case EqualTo(ColumnName(column), value) if columnInfo.isPredicateValueColumn(column) && Option(value).isDefined =>
      Some(Set(SinglePredicateValueIsIn(column, Set(value))))
    case EqualTo(ColumnName(column), value) if columnInfo.isObjectValueColumn(column) && Option(value).isDefined =>
      Some(Set(ObjectValueIsIn(value)) ++ columnInfo.getObjectType(column).map(t => Set(ObjectTypeIsIn(t))).getOrElse(Set.empty))
    case EqualTo(ColumnName(column), value) if columnInfo.isObjectTypeColumn(column) && Option(value).isDefined =>
      Some(Set(ObjectTypeIsIn(value.toString)))

    case In(ColumnName(column), values)
      if columnInfo.isSubjectColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Set(SubjectIsIn(values.map(value => Uid(value.toLong)): _*)))
    case In(ColumnName(column), values)
      if columnInfo.isPredicateColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Set(IntersectPredicateNameIsIn(values.map(_.toString): _*)))
    case In(ColumnName(column), values)
      if columnInfo.isPredicateValueColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Set(SinglePredicateValueIsIn(column, values.toSet)))
    case In(ColumnName(column), values)
      if columnInfo.isObjectValueColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Set(ObjectValueIsIn(values.toSet[Any])) ++ columnInfo.getObjectType(column).map(t => Set(ObjectTypeIsIn(t))).getOrElse(Set.empty))
    case In(ColumnName(column), values)
      if columnInfo.isObjectTypeColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Set(ObjectTypeIsIn(values.map(_.toString): _*)))

    case _ => None
  }

}

object FilterTranslator {

  def simplify(filters: Set[Filter]): Set[Filter] = {
    // first simplification intersects filters with set arguments
    val intersected =
      filters
        .groupBy(_.getClass)
        .flatMap { case (cls, filters) =>
          cls match {
            case cls if cls == classOf[SubjectIsIn] => Set[Filter](filters.map(_.asInstanceOf[SubjectIsIn]).reduce(intersectSubjectIsIn))
            case cls if cls == classOf[IntersectPredicateNameIsIn] => Set[Filter](filters.map(_.asInstanceOf[IntersectPredicateNameIsIn]).reduce(intersectPredicateNameIsIn))
            case cls if cls == classOf[IntersectPredicateValueIsIn] => Set[Filter](filters.map(_.asInstanceOf[IntersectPredicateValueIsIn]).reduce(intersectPredicateValueIsIn))
            case cls if cls == classOf[SinglePredicateValueIsIn] =>
              filters
                .map(_.asInstanceOf[SinglePredicateValueIsIn])
                .groupBy(_.name)
                .mapValues(fs => Set[Filter](fs.reduce(intersectSinglePredicateValueIsIn)))
                .values.flatten
            case cls if cls == classOf[ObjectTypeIsIn] => Set[Filter](filters.map(_.asInstanceOf[ObjectTypeIsIn]).reduce(intersectObjectTypeIsIn))
            case cls if cls == classOf[ObjectValueIsIn] => Set[Filter](filters.map(_.asInstanceOf[ObjectValueIsIn]).reduce(intersectObjectValueIsIn))
            case _ => filters
          }
        }
        .toSet

    // we can simplify some trivial expressions
    val trivialized =
      intersected
        .flatMap {
          case AlwaysTrue => None
          case SubjectIsIn(s) if s.isEmpty => Some(AlwaysFalse)
          case IntersectPredicateNameIsIn(s) if s.isEmpty => Some(AlwaysFalse)
          case IntersectPredicateValueIsIn(p, v) if p.isEmpty || v.isEmpty => Some(AlwaysFalse)
          case ObjectTypeIsIn(s) if s.isEmpty => Some(AlwaysFalse)
          case ObjectValueIsIn(s) if s.isEmpty => Some(AlwaysFalse)
          case f => Some(f)
        }

    // we simplify a single PredicateNameIsIn and ObjectValueIsIn into a PredicateValueIsIn
    val singlePredicateName = Some(trivialized.filter(_.isInstanceOf[PredicateNameIsIn])).filter(_.size == 1).map(_.head.asInstanceOf[PredicateNameIsIn])
    val singleObjectValue = Some(trivialized.filter(_.isInstanceOf[ObjectValueIsIn])).filter(_.size == 1).map(_.head.asInstanceOf[ObjectValueIsIn])
    val predicateValues = trivialized.filter(_.isInstanceOf[PredicateValueIsIn]).map(_.asInstanceOf[PredicateValueIsIn].names).toSet

    val simplified =
      trivialized
        .flatMap {
          // drop PredicateNameIsIn if there is a PredicateValueIsIn that contains all its names
          case f: PredicateNameIsIn if predicateValues.exists(f.names.diff(_).isEmpty) =>
            None
          // replace PredicateNameIs with SinglePredicateValueIsIn combining PredicateNameIs with PredicateValueIsIn
          case PredicateNameIs(name) if singlePredicateName.isDefined && singleObjectValue.isDefined =>
            Some[Filter](SinglePredicateValueIsIn(name, singleObjectValue.get.values))
          // replace IntersectPredicateNameIsIn with IntersectPredicateValueIsIn combining IntersectPredicateNameIsIn with PredicateValueIsIn
          case IntersectPredicateNameIsIn(_) if singlePredicateName.isDefined && singleObjectValue.isDefined =>
            Some[Filter](IntersectPredicateValueIsIn(singlePredicateName.get.names, singleObjectValue.get.values))
          // drop ObjectValueIsIn, we have replaced the PredicateNameIs / IntersectPredicateNameIsIn filter above
          case ObjectValueIsIn(_) if singlePredicateName.isDefined && singleObjectValue.isDefined =>
            None
          // keep all others
          case f => Some(f)
        }

    if (simplified.contains(AlwaysFalse))
      Set(AlwaysFalse)
    else
      simplified
  }

  /**
   * Simplifies given promised and optional filters. This method
   * guarantees that all returned filters are supported and cover all given prmised filters.
   * @param filters filters
   * @param supported function to indicate support of filters
   * @return simplified filters
   */
  def simplify(filters: Filters, supported: Set[Filter] => Boolean): Filters = {
    val allSimplified = simplify(filters)
    val promisedSimplified = simplify(filters.promised)
    val optionalSimplified = simplify(filters.optional)

    // we could use allSimplified filters even if some are not supported,
    // as long as it is guaranteed that they fully cover the promised filters
    // but we have no way to indicate this here, which would add a bit of extra logic
    if (supported(allSimplified)) {
      Filters.from(allSimplified)
    } else {
      Filters.from(
        if (supported(promisedSimplified)) promisedSimplified else filters.promised,
        if (supported(optionalSimplified)) optionalSimplified else filters.optional
      )
    }
  }

  def intersectSubjectIsIn(left: SubjectIsIn, right: SubjectIsIn): SubjectIsIn = SubjectIsIn(left.uids.intersect(right.uids))

  def intersectPredicateNameIsIn(left: IntersectPredicateNameIsIn, right: IntersectPredicateNameIsIn): IntersectPredicateNameIsIn = IntersectPredicateNameIsIn(left.names.intersect(right.names))

  def intersectPredicateValueIsIn(left: IntersectPredicateValueIsIn, right: IntersectPredicateValueIsIn): IntersectPredicateValueIsIn = IntersectPredicateValueIsIn(left.names.intersect(right.names), left.values.intersect(right.values))

  def intersectSinglePredicateValueIsIn(left: SinglePredicateValueIsIn, right: SinglePredicateValueIsIn): SinglePredicateValueIsIn = SinglePredicateValueIsIn(left.name, left.values.intersect(right.values))

  def intersectObjectTypeIsIn(left: ObjectTypeIsIn, right: ObjectTypeIsIn): ObjectTypeIsIn = ObjectTypeIsIn(left.types.intersect(right.types))

  def intersectObjectValueIsIn(left: ObjectValueIsIn, right: ObjectValueIsIn): ObjectValueIsIn = ObjectValueIsIn(left.values.intersect(right.values))
}
