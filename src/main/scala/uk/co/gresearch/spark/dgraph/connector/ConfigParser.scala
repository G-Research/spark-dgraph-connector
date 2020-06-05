package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait ConfigParser {

  def getStringOption(option: String, options: CaseInsensitiveStringMap): Option[String] =
    Option(options.get(option))

  def getStringOption(option: String, options: CaseInsensitiveStringMap, default: String): String =
    getStringOption(option, options).getOrElse(default)

  def getIntOption(option: String, options: CaseInsensitiveStringMap): Option[Int] =
    Option(options.get(option)).map(_.toInt)

  def getIntOption(option: String, options: CaseInsensitiveStringMap, default: Int): Int =
    getIntOption(option, options).getOrElse(default)

}
