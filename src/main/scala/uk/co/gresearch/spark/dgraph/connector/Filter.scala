package uk.co.gresearch.spark.dgraph.connector

/**
 * A dgraph connector equivalent for spark's sql.sources.Filter.
 */
abstract class Filter()

case class SubjectIsIn(uids: Set[Uid]) extends Filter
case class PredicateNameIsIn(names: Set[String]) extends Filter
case class ObjectTypeIsIn(types: Set[String]) extends Filter
case class ObjectValueIsIn(values: Set[Any]) extends Filter
