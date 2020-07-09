package uk.co.gresearch.spark.dgraph.connector.model

import com.google.gson.{JsonArray, JsonObject, JsonPrimitive}
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.model.TestChunkIterator.getChunk
import uk.co.gresearch.spark.dgraph.connector.{Chunk, Uid}

class TestChunkIterator extends FunSpec {

  describe("ChunkIterator") {

    val zero = Uid("0x0")

    it("should handle empty result set") {
      val chunks = Map(Chunk(zero, 10) -> new JsonArray())
      val it = ChunkIterator(zero, None, 10, chunk => chunks.getOrElse(chunk, fail(s"unexpected chunk: $chunk")))
      assert(it.toSeq === Seq.empty)
    }

    it("should handle empty chunk") {
      val uids = 0 until 10 map { id => Uid(id * 7) }
      val chunk = getChunk(uids)
      // last uid of first chunk is 9*7 = 73, 0x3f in hex
      val chunks = Map(Chunk(zero, 10) -> chunk, Chunk(Uid("0x3f"), 10) -> new JsonArray())
      val it = ChunkIterator(zero, None, 10, chunk => chunks.getOrElse(chunk, fail(s"unexpected chunk: $chunk")))
      assert(it.toSeq === Seq(chunk))
    }

    it("should return all chunks") {
      val uids = 1 to 13 map { id => Uid(id * 7) }
      val uidChunks = uids.grouped(5).toSeq

      val chunks =
        uidChunks
          .map(getChunk)
          .zip(Seq(zero) ++ uidChunks.map(_.last).dropRight(1))
          .map {
            case (chunk, uid) => Chunk(uid, 5) -> chunk
          }.toMap

      val it = ChunkIterator(zero, None, 5, chunk => chunks.getOrElse(chunk, fail(s"unexpected chunk: $chunk")))
      assert(it.toSeq === chunks.values.toStream)
    }

    it("should limit chunk size of last chunk in uid range") {
      val chunks = Map(
        Chunk(Uid(0), 10) -> getChunk(1 to 10 map { id => Uid(id) }),
        Chunk(Uid(10), 4) -> getChunk(11 to 14 map { id => Uid(id) })
      )

      val it = ChunkIterator(zero, Some(Uid(15)), 10, chunk => chunks.getOrElse(chunk, fail(s"unexpected chunk: $chunk")))
      assert(it.toSeq === chunks.values.toStream)
    }

    it("should fail on invalid size") {
      assertThrows[IllegalArgumentException] {
        ChunkIterator(zero, None, 0, null)
      }
      assertThrows[IllegalArgumentException] {
        ChunkIterator(zero, None, -1, null)
      }
      assertThrows[IllegalArgumentException] {
        ChunkIterator(zero, None, Int.MinValue, null)
      }
    }

    // TODO: test with non-none after
  }

}

object TestChunkIterator {
  def getChunk(uids: Seq[Uid]): JsonArray = {
    val chunk = new JsonArray()
    uids.foreach { uid =>
      val element = new JsonObject()
      element.add("uid", new JsonPrimitive(uid.toHexString))
      chunk.add(element)
    }
    chunk
  }
}
