package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql
import org.apache.spark.sql.sources.{EqualTo, In}
import uk.co.gresearch.spark.dgraph.connector.FilterTranslator._
import uk.co.gresearch.spark.dgraph.connector.encoder.ColumnInfo

case class FilterTranslator(columnInfo: ColumnInfo) {

  /**
   * Translates the spark filter to some dgraph connector filters, or None if no translation available.
   * @param filter spark filter
   * @return Some dgraph filters
   */
  def translate(filter: sql.sources.Filter): Option[Seq[Filter]] = filter match {
    case EqualTo(column, value) if columnInfo.isSubjectColumn(column) && Option(value).isDefined =>
      Some(Seq(SubjectIsIn(Set(Uid(value.toLong)))))
    case EqualTo(column, value) if columnInfo.isPredicateColumn(column) && Option(value).isDefined =>
      Some(Seq(PredicateNameIsIn(Set(value.toString))))
    case EqualTo(column, value) if columnInfo.isPredicateValueColumn(column) && Option(value).isDefined =>
      Some(Seq(PredicateNameIsIn(Set(column)), ObjectValueIsIn(Set(value))))
    case EqualTo(column, value) if columnInfo.isObjectValueColumn(column) && Option(value).isDefined =>
      Some(Seq(ObjectValueIsIn(Set(value.toString))) ++ Seq(columnInfo.getObjectType(column).map(t => ObjectTypeIsIn(Set(t))).get))
    case EqualTo(column, value) if columnInfo.isObjectTypeColumn(column) && Option(value).isDefined =>
      Some(Seq(ObjectTypeIsIn(Set(value.toString))))

    case In(column, values)
      if columnInfo.isSubjectColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Seq(SubjectIsIn(values.map(value => Uid(value.toLong)).toSet)))
    case In(column, values)
      if columnInfo.isPredicateColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Seq(PredicateNameIsIn(values.map(_.toString).toSet)))
    case In(column, values)
      if columnInfo.isPredicateValueColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Seq(PredicateNameIsIn(Set(column)), ObjectValueIsIn(values.toSet)))
    case In(column, values)
      if columnInfo.isObjectValueColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Seq(ObjectValueIsIn(values.map(_.toString).toSet)) ++ Seq(columnInfo.getObjectType(column).map(t => ObjectTypeIsIn(Set(t))).get))
    case In(column, values)
      if columnInfo.isObjectTypeColumn(column) &&
        // check for non-null null-less non-empty values array
        Option(values).map(_.filter(Option(_).isDefined)).exists(_.length > 0) =>
      Some(Seq(ObjectTypeIsIn(values.map(_.toString).toSet)))

    case _ => None
  }

}

object FilterTranslator {
  implicit class AnyValue(value: Any) {
    def toLong: Long = value match {
      case v: Int => v.toLong
      case v: Long => v
      case v: String => v.toLong
      case _ => value.asInstanceOf[Long]
    }
  }
}
