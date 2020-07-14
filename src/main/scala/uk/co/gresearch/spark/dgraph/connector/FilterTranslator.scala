package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql
import org.apache.spark.sql.sources._
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.encoder.ColumnInfo

case class FilterTranslator(columnInfo: ColumnInfo) {

  /**
   * Translates the spark filter to some dgraph connector filters, or None if no translation available.
   * @param filter spark filter
   * @return Some dgraph filters
   */
  def translate(filter: sql.sources.Filter): Option[Seq[Filter]] = filter match {
//    case IsNotNull(column) if columnInfo.isPredicateValueColumn(column) =>
//      Some(Seq(PredicateNameIsIn(column)))
    case IsNotNull(column) if columnInfo.isObjectValueColumn(column) && columnInfo.getObjectType(column).isDefined =>
      Some(Seq(ObjectTypeIsIn(columnInfo.getObjectType(column).get)))

    case EqualTo(column, value) if columnInfo.isSubjectColumn(column) && Option(value).isDefined =>
      Some(Seq(SubjectIsIn(Uid(value.toLong))))
    case EqualTo(column, value) if columnInfo.isPredicateColumn(column) && Option(value).isDefined =>
      Some(Seq(PredicateNameIsIn(value.toString)))
//    case EqualTo(column, value) if columnInfo.isPredicateValueColumn(column) && Option(value).isDefined =>
//      Some(Seq(PredicateNameIsIn(column), ObjectValueIsIn(value)))
    case EqualTo(column, value) if columnInfo.isObjectValueColumn(column) && Option(value).isDefined =>
      Some(Seq(ObjectValueIsIn(value)) ++ columnInfo.getObjectType(column).map(t => Seq(ObjectTypeIsIn(t))).getOrElse(Seq.empty))
    case EqualTo(column, value) if columnInfo.isObjectTypeColumn(column) && Option(value).isDefined =>
      Some(Seq(ObjectTypeIsIn(value.toString)))

    case In(column, values)
      if columnInfo.isSubjectColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Seq(SubjectIsIn(values.map(value => Uid(value.toLong)): _*)))
    case In(column, values)
      if columnInfo.isPredicateColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Seq(PredicateNameIsIn(values.map(_.toString): _*)))
//    case In(column, values)
//      if columnInfo.isPredicateValueColumn(column) &&
//        // check for non-null null-less non-empty values array
//        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
//      Some(Seq(PredicateNameIsIn(column), ObjectValueIsIn(values: _*)))
    case In(column, values)
      if columnInfo.isObjectValueColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Seq(ObjectValueIsIn(values.toSet[Any])) ++ columnInfo.getObjectType(column).map(t => Seq(ObjectTypeIsIn(t))).getOrElse(Seq.empty))
    case In(column, values)
      if columnInfo.isObjectTypeColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Seq(ObjectTypeIsIn(values.map(_.toString): _*)))

    case sql.sources.AlwaysTrue => Some(Seq(AlwaysTrue))
    case sql.sources.AlwaysFalse => Some(Seq(AlwaysFalse))

    case _ => None
  }

}

object FilterTranslator {

  def simplify(filters: Seq[Filter]): Seq[Filter] = {
    // first simplification intersetcs filters with set arguments
    val intersected =
      filters
        .groupBy(_.getClass)
        .flatMap { case (cls, filters) =>
          cls match {
            case cls if cls == classOf[SubjectIsIn] => Seq[Filter](filters.map(_.asInstanceOf[SubjectIsIn]).reduce(intersectSubjectIsIn))
            case cls if cls == classOf[PredicateNameIsIn] => Seq[Filter](filters.map(_.asInstanceOf[PredicateNameIsIn]).reduce(intersectPredicateNameIsIn))
            case cls if cls == classOf[PredicateValueIsIn] => Seq[Filter](filters.map(_.asInstanceOf[PredicateValueIsIn]).reduce(intersectPredicateValueIsIn))
            case cls if cls == classOf[ObjectTypeIsIn] => Seq[Filter](filters.map(_.asInstanceOf[ObjectTypeIsIn]).reduce(intersectObjectTypeIsIn))
            case cls if cls == classOf[ObjectValueIsIn] => Seq[Filter](filters.map(_.asInstanceOf[ObjectValueIsIn]).reduce(intersectObjectValueIsIn))
            case _ => filters
          }
        }
        .toSeq

    // we can simplify some trivial expressions
    val trivialized =
      intersected
        .flatMap {
          case AlwaysTrue => None
          case SubjectIsIn(s) if s.isEmpty => Some(AlwaysFalse)
          case PredicateNameIsIn(s) if s.isEmpty => Some(AlwaysFalse)
          case ObjectTypeIsIn(s) if s.isEmpty => Some(AlwaysFalse)
          case ObjectValueIsIn(s) if s.isEmpty => Some(AlwaysFalse)
          case f => Some(f)
        }

    // we simplify a single PredicateNameIsIn and ObjectValueIsIn into a PredicateValueIsIn
    val singlePredicateName = Some(trivialized.filter(_.isInstanceOf[PredicateNameIsIn])).filter(_.size == 1).map(_.head.asInstanceOf[PredicateNameIsIn])
    val singleObjectValue = Some(trivialized.filter(_.isInstanceOf[ObjectValueIsIn])).filter(_.size == 1).map(_.head.asInstanceOf[ObjectValueIsIn])

    val simplified =
      trivialized
        .flatMap {
          case PredicateNameIsIn(_) if singlePredicateName.isDefined && singleObjectValue.isDefined =>
            // drop PredicateNameIsIn, we will replace the ObjectValueIsIn filter
            None
          case ObjectValueIsIn(_) if singlePredicateName.isDefined && singleObjectValue.isDefined =>
            // replace ObjectValueIsIn with PredicateValueIsIn combining PredicateNameIsIn with PredicateValueIsIn
            Some[Filter](PredicateValueIsIn(singlePredicateName.get.names, singleObjectValue.get.values))
          case f => Some(f)
        }

    if (simplified.contains(AlwaysFalse))
      Seq(AlwaysFalse)
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
  def simplify(filters: Filters, supported: Seq[Filter] => Boolean): Filters = {
    val allSimplified = simplify(filters)
    val promisedSimplified = simplify(filters.promised)
    val optionalSimplified = simplify(filters.optional)

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

  def intersectPredicateNameIsIn(left: PredicateNameIsIn, right: PredicateNameIsIn): PredicateNameIsIn = PredicateNameIsIn(left.names.intersect(right.names))

  def intersectPredicateValueIsIn(left: PredicateValueIsIn, right: PredicateValueIsIn): PredicateValueIsIn = PredicateValueIsIn(left.names.intersect(right.names), left.values.intersect(right.values))

  def intersectObjectTypeIsIn(left: ObjectTypeIsIn, right: ObjectTypeIsIn): ObjectTypeIsIn = ObjectTypeIsIn(left.types.intersect(right.types))

  def intersectObjectValueIsIn(left: ObjectValueIsIn, right: ObjectValueIsIn): ObjectValueIsIn = ObjectValueIsIn(left.values.intersect(right.values))
}
