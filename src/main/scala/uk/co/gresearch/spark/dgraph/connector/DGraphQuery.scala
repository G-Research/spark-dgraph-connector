package uk.co.gresearch.spark.dgraph.connector

object DGraphQuery {

  def forAllPropertiesAndEdges(resultName: String): String =
    s"""{
       |  ${resultName} (func: has(dgraph.type)) {
       |    uid
       |    expand(_all_) {
       |      uid
       |    }
       |  }
       |}""".stripMargin

  def forAllPropertiesAndEdges(resultName: String, schema: Schema): String = {
    val filter =
      Option(schema.predicatesTypes)
        .filter(_.nonEmpty)
        .map(_.keys.map(p => s"has($p)").mkString(" OR "))
        .getOrElse("eq(true, false")

    val predicates =
      Option(schema.predicatesTypes)
        .filter(_.nonEmpty)
        .map(t =>
          t.map {
            case (predicate, "uid") => s"    $predicate { uid }"
            case (predicate, _____) => s"    $predicate"
          }.mkString("\n") + "\n"
        ).getOrElse("")

    s"""{
       |  ${resultName} (func: has(dgraph.type)) @filter(${filter}) {
       |    uid
       |${predicates}  }
       |}""".stripMargin
  }

}
