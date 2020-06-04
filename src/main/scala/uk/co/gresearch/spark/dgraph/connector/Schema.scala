package uk.co.gresearch.spark.dgraph.connector

case class Schema(predicates: Set[Predicate]) {

  val predicateMap: Map[String, Predicate] = predicates.map(p => p.predicateName -> p).toMap

  def getObjectType(predicateName: String): Option[String] = predicateMap.get(predicateName).map(_.typeName)

  def filter(condition: (Predicate) => Boolean): Schema = Schema(predicates.filter(condition))

}
