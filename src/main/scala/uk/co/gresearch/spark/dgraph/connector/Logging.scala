package uk.co.gresearch.spark.dgraph.connector

import java.text.NumberFormat

import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{LogManager, Logger}

trait Logging {

  val loggingStringMaxLength = 1000
  val loggingStringAbbreviateMiddle = "[…]"

  lazy val loggingFormat: NumberFormat = NumberFormat.getInstance()

  @transient
  lazy val log: Logger = LogManager.getLogger(this.getClass)

  def abbreviate(string: String): String =
    StringUtils.abbreviateMiddle(string, loggingStringAbbreviateMiddle, loggingStringMaxLength)

}
