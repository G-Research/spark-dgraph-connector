package uk.co.gresearch.spark.dgraph.connector.encoder

import com.google.gson.{Gson, JsonArray, JsonObject}
import uk.co.gresearch.spark.dgraph.connector.{Json, Logging}

import scala.jdk.CollectionConverters._

trait JsonGraphQlResultParser extends Logging {

  /**
   * Parses the given Json result and returns the result array.
   * @param json
   *   Json result
   * @param member
   *   member in the json that has the result
   * @return
   *   Json array
   */
  def getResult(json: Json, member: String): JsonArray = {
    try {
      new Gson().fromJson(json.string, classOf[JsonObject]).getAsJsonArray(member)
    } catch {
      case t: Throwable =>
        log.error(s"failed to parse element '$member' in dgraph json result: ${abbreviate(json.string)}")
        throw t
    }
  }

  /**
   * Provides the result elements as JsonObjects
   * @param result
   *   Json array
   * @return
   *   result elements
   */
  def getNodes(result: JsonArray): Iterator[JsonObject] =
    result
      .iterator()
      .asScala
      .map(_.getAsJsonObject)
}
