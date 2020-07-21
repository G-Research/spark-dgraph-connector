package uk.co.gresearch.spark.dgraph.connector

/**
 * A dgraph connector equivalent for spark's sql.sources.Filter.
 */
abstract class Filter()

case class Filters(promised: Seq[Filter], optional: Seq[Filter]) extends Seq[Filter] {
  val filters: Seq[Filter] = promised ++ optional

  override def apply(idx: Int): Filter = filters(idx)

  override def length: Int = filters.length

  override def iterator: Iterator[Filter] = filters.iterator
}

object Filters {
  def from(promised: Seq[Filter], optional: Seq[Filter] = Seq.empty): Filters = Filters(promised, optional)
  def fromPromised(promised: Filter, morePromised: Filter*): Filters = Filters(Seq(promised) ++ morePromised, Seq.empty)
  def fromOptional(optional: Filter, moreOptional: Filter*): Filters = Filters(Seq.empty, Seq(optional) ++ moreOptional)
}

object EmptyFilters extends Filters(Seq.empty, Seq.empty)

case class AlwaysTrue() extends Filter
case class AlwaysFalse() extends Filter
case class SubjectIsIn(uids: Set[Uid]) extends Filter

// PredicateNameIsIn comes with two semantics: one that has to be intersected and one that cannot
abstract class PredicateNameIsIn(val names: Set[String]) extends Filter
case class IntersectPredicateNameIsIn(override val names: Set[String]) extends PredicateNameIsIn(names)
case class PredicateNameIs(name: String) extends PredicateNameIsIn(Set(name))

// PredicateValueIsIn comes with two semantics: one that has to be intersected and one that cannot
abstract class PredicateValueIsIn(val names: Set[String], val values: Set[Any]) extends Filter
case class IntersectPredicateValueIsIn(override val names: Set[String], override val values: Set[Any]) extends PredicateValueIsIn(names, values)
case class SinglePredicateValueIsIn(name: String, override val values: Set[Any]) extends PredicateValueIsIn(Set(name), values)

case class ObjectTypeIsIn(types: Set[String]) extends Filter
case class ObjectValueIsIn(values: Set[Any]) extends Filter

object AlwaysTrue extends AlwaysTrue
object AlwaysFalse extends AlwaysFalse

object SubjectIsIn {
  def apply(uids: Uid*): SubjectIsIn = new SubjectIsIn(uids.toSet)
}

object IntersectPredicateNameIsIn {
  def apply(names: String*): IntersectPredicateNameIsIn = new IntersectPredicateNameIsIn(names.toSet)
}

object ObjectTypeIsIn {
  def apply(types: String*): ObjectTypeIsIn = new ObjectTypeIsIn(types.toSet)
}

object ObjectValueIsIn {
  def apply(values: Any*): ObjectValueIsIn = new ObjectValueIsIn(values.toSet)
}
