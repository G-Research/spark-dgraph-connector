package uk.co.gresearch.spark.dgraph.connector

/**
 * A dgraph connector equivalent for spark's sql.sources.Filter.
 */
abstract class Filter()

case class SubjectIsIn(uids: Set[Uid]) extends Filter
case class PredicateNameIsIn(names: Set[String]) extends Filter
case class ObjectTypeIsIn(types: Set[String]) extends Filter
case class ObjectValueIsIn(values: Set[Any]) extends Filter

object SubjectIsIn {
  def apply(uids: Uid*): SubjectIsIn = new SubjectIsIn(uids.toSet)
}

object PredicateNameIsIn {
  def apply(names: String*): PredicateNameIsIn = new PredicateNameIsIn(names.toSet)
}

object ObjectTypeIsIn {
  def apply(types: String*): ObjectTypeIsIn = new ObjectTypeIsIn(types.toSet)
}

object ObjectValueIsIn {
  def apply(values: Any*): ObjectValueIsIn = new ObjectValueIsIn(values.toSet)
}
