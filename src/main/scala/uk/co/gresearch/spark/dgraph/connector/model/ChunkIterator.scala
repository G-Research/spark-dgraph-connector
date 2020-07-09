package uk.co.gresearch.spark.dgraph.connector.model

import com.google.gson.{JsonArray, JsonElement}
import uk.co.gresearch.spark.dgraph.connector.{Chunk, Uid}
import uk.co.gresearch.spark.dgraph.connector.model.ChunkIterator.getLastUid

case class ChunkIterator(after: Uid, chunkSize: Int, readChunk: Chunk => JsonArray) extends Iterator[JsonArray] {

  if (chunkSize <= 0)
    throw new IllegalArgumentException(s"Chunk size must be larger than zero: $chunkSize")

  var nextChunk: Option[Chunk] = Some(Chunk(after, chunkSize))
  var nextValue: JsonArray = _

  // read first chunk
  next()

  override def hasNext: Boolean = nextValue.size() > 0

  override def next(): JsonArray = {
    val value = nextValue

    if (nextChunk.isDefined) {
      nextValue = readChunk(nextChunk.get)
      nextChunk =
        if (nextValue.size() >= nextChunk.get.length) {
          Some(nextChunk.get.withAfter(getLastUid(nextValue)))
        } else {
          None
        }
    } else {
      nextValue = new JsonArray()
      nextChunk = None
    }

    value
  }

}

object ChunkIterator {
  def getLastUid(array: JsonArray): Uid = getUid(array.get(array.size() - 1))

  def getUid(element: JsonElement): Uid = Uid(element.getAsJsonObject.get("uid").getAsString)
}
