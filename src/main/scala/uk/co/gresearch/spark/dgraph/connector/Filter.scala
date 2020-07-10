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
case class PredicateNameIsIn(names: Set[String]) extends Filter
case class ObjectTypeIsIn(types: Set[String]) extends Filter
case class ObjectValueIsIn(values: Set[Any]) extends Filter

object AlwaysTrue extends AlwaysTrue
object AlwaysFalse extends AlwaysFalse

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
