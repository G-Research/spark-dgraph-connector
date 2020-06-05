package uk.co.gresearch.spark.dgraph.connector

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait TargetsConfigParser extends ConfigParser {

  protected def getTargets(options: CaseInsensitiveStringMap): Seq[Target] = {
    val objectMapper = new ObjectMapper()
    val fromTargets = Seq(TargetsOption, "paths").flatMap(option =>
      getStringOption(option, options).map { pathStr =>
        objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
      }.getOrElse(Seq.empty[String])
    )

    val fromTarget = Seq(TargetOption, "path").flatMap(getStringOption(_, options))

    val allTargets = fromTargets ++ fromTarget
    if (allTargets.isEmpty)
      throw new IllegalArgumentException("No Dgraph servers provided, provide targets via " +
        "DataFrameReader.load(…) or DataFrameReader.option(TargetOption, …)"
      )

    allTargets.map(Target)
  }

}
