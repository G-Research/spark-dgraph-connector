package uk.co.gresearch.spark.dgraph.connector

object Query {

  def forAllProperties(resultName: String, uids: Option[UidRange]): String = {
    val pagination =
      uids.map(range => s", first: ${range.length}, offset: ${range.first}").getOrElse("")

    s"""{
       |  ${resultName} (func: has(dgraph.type)$pagination) {
       |    uid
       |    expand(_all_)
       |  }
       |}""".stripMargin
  }

  def forAllPropertiesAndEdges(resultName: String, uids: Option[UidRange]): String = {
    val pagination =
      uids.map(range => s", first: ${range.length}, offset: ${range.first}").getOrElse("")

    s"""{
       |  ${resultName} (func: has(dgraph.type)$pagination) {
       |    uid
       |    expand(_all_) {
       |      uid
       |    }
       |  }
       |}""".stripMargin
  }


  def forPropertiesAndEdges(resultName: String, predicates: Set[Predicate], uids: Option[UidRange]): String = {
      val pagination =
        uids.map(range => s", first: ${range.length}, offset: ${range.first}").getOrElse("")

      val filter =
          Option(predicates)
            .filter(_.nonEmpty)
            .map(_.map(p => s"has(${p.predicateName})").mkString(" OR "))
            // an empty predicates set must return empty result set
            .orElse(Some("eq(true, false)"))
            .map(filter => s"@filter(${filter}) ")
            .get

      val predicatesPaths =
        Option(predicates)
          .filter(_.nonEmpty)
          .map(t =>
            t.map {
              case Predicate(predicate, "uid") => s"    $predicate { uid }"
              case Predicate(predicate, _____) => s"    $predicate"
            }.mkString("\n") + "\n"
          ).getOrElse("")

      s"""{
         |  ${resultName} (func: has(dgraph.type)${pagination}) ${filter}{
         |    uid
         |${predicatesPaths}  }
         |}""".stripMargin
    }

}
