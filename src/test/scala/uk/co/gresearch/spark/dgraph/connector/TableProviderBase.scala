package uk.co.gresearch.spark.dgraph.connector

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait TableProviderBase extends TableProvider with DataSourceRegister {

  protected def getTargets(map: CaseInsensitiveStringMap): Seq[Target] = {
    val objectMapper = new ObjectMapper()
    val targets = Option(map.get(TargetsOption)).map { pathStr =>
      objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
    }.getOrElse(Seq.empty)
    (targets ++ Option(map.get(TargetOption))).map(Target)
  }

}
