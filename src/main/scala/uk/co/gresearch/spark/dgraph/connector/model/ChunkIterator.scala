/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector.model

import com.google.gson.{JsonArray, JsonElement}
import uk.co.gresearch.spark.dgraph.connector.{Chunk, Uid}
import uk.co.gresearch.spark.dgraph.connector.model.ChunkIterator.getLastUid

case class ChunkIterator(after: Uid, until: Option[Uid], chunkSize: Int, readChunk: Chunk => JsonArray)
  extends Iterator[JsonArray] {

  if (chunkSize <= 0)
    throw new IllegalArgumentException(s"Chunk size must be larger than zero: $chunkSize")

  def getChunkSize(after: Uid, until: Uid, chunkSize: Int): Int =
    math.min((until.uid - after.uid - 1).toInt, chunkSize)

  val firstChunkSize: Int = until.map(u => getChunkSize(after, u, chunkSize)).getOrElse(chunkSize)

  var nextChunk: Option[Chunk] = Some(Chunk(after, firstChunkSize))
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
          val next = nextChunk.get.withAfter(getLastUid(nextValue))
          // limit chunk length by until
          until.map(u =>
            // next chunk might be empty
            if (next.after == u.before) {
              None
            } else {
              Some(next.withLength(getChunkSize(next.after, until.get, chunkSize)))
            }).getOrElse(Some(next))
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
