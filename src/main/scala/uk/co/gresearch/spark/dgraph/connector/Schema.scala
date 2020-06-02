package uk.co.gresearch.spark.dgraph.connector

case class Schema(predicatesTypes: Map[String, String]) {
  def getObjectType(predicate: String): Option[String] = predicatesTypes.get(predicate)
}
