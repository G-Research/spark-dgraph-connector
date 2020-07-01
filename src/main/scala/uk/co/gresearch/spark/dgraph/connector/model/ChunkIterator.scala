package uk.co.gresearch.spark.dgraph.connector.model

import com.google.gson.JsonArray
import uk.co.gresearch.spark.dgraph.connector.{Chunk, Uid}

case class ChunkIterator(chunkSize: Int, readChunk: Chunk => JsonArray) extends Iterator[JsonArray] {

  if (chunkSize <= 0)
    throw new IllegalArgumentException(s"Chunk size must be larger than zero: $chunkSize")

  var nextChunk: Option[Chunk] = Some(Chunk(Uid("0x0"), chunkSize))
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
          Some(nextChunk.get.copy(after = getLastUid(nextValue)))
        } else {
          None
        }
    } else {
      nextValue = new JsonArray()
      nextChunk = None
    }

    value
  }

  /**
   * Extracts the uid of the last element of the Json array.
   * @param array Json array
   * @return uid of the last element
   */
  private def getLastUid(array: JsonArray): Uid = {
    val uidString = array.get(array.size()-1).getAsJsonObject.get("uid").getAsString
    Uid(uidString)
  }

}
