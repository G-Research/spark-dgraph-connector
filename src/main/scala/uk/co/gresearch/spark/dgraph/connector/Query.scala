package uk.co.gresearch.spark.dgraph.connector

object Query {

  def forAllPropertiesAndEdges(resultName: String): String =
    s"""{
       |  ${resultName} (func: has(dgraph.type)) {
       |    uid
       |    expand(_all_) {
       |      uid
       |    }
       |  }
       |}""".stripMargin

  def forAllPropertiesAndEdges(resultName: String, predicates: Option[Set[Predicate]]): String = {
    val filter =
      predicates
        .filter(_.nonEmpty)
        .map(_.map(p => s"has(${p.predicateName})").mkString(" OR "))
        .getOrElse("eq(true, false")

    val predicatesPaths =
      predicates
        .filter(_.nonEmpty)
        .map(t =>
          t.map {
            case Predicate(predicate, "uid") => s"    $predicate { uid }"
            case Predicate(predicate, _____) => s"    $predicate"
          }.mkString("\n") + "\n"
        ).getOrElse("")

    s"""{
       |  ${resultName} (func: has(dgraph.type)) @filter(${filter}) {
       |    uid
       |${predicatesPaths}  }
       |}""".stripMargin
  }

}
