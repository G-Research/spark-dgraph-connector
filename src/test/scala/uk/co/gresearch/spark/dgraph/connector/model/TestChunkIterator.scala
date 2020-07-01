package uk.co.gresearch.spark.dgraph.connector.model

import com.google.gson.{JsonArray, JsonObject, JsonPrimitive}
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{Chunk, Uid}

import scala.collection.JavaConverters._

class TestChunkIterator extends FunSpec{

  describe("ChunkIterator") {

    it("should handle empty result set") {
      val chunks = Map(Chunk(Uid("0x0"), 10) -> new JsonArray())
      val it = ChunkIterator(10, chunk => chunks.getOrElse(chunk, fail(s"unexpected chunk: $chunk")))
      assert(it.toSeq === Seq.empty)
    }

    it("should handle empty chunk") {
      val uids = 0 until 10 map { id => Uid(id*7) }

      val chunk = new JsonArray()
      uids.foreach {uid =>
        val element = new JsonObject()
        element.add("uid", new JsonPrimitive(uid.toHexString))
        chunk.add(element)
      }

      // last uid of first chunk is 9*7 = 73, 0x3f in hex
      val chunks = Map(Chunk(Uid("0x0"), 10) -> chunk, Chunk(Uid("0x3f"), 10) -> new JsonArray())
      val it = ChunkIterator(10, chunk => chunks.getOrElse(chunk, fail(s"unexpected chunk: $chunk")))
      assert(it.toSeq === Seq(chunk))
    }

    it("should return all chunks") {
      val uids = 1 to 13 map { id => Uid(id * 7) }
      val uidChunks = uids.grouped(5).toSeq

      val chunks = uidChunks.map { uids =>
        val chunk = new JsonArray()
        uids.foreach { uid =>
          val element = new JsonObject()
          element.add("uid", new JsonPrimitive(uid.toHexString))
          chunk.add(element)
        }
        chunk
      }.zip(Seq(Uid("0x0")) ++ uidChunks.map(_.last).dropRight(1)).map {
        case (chunk, uid) => Chunk(uid, 5) -> chunk
      }.toMap

      val it = ChunkIterator(5, chunk => chunks.getOrElse(chunk, fail(s"unexpected chunk: $chunk")))
      assert(it.toSeq === chunks.values.toStream)
    }

    it("should fail on invalid size") {
      assertThrows[IllegalArgumentException] { ChunkIterator(0, null) }
      assertThrows[IllegalArgumentException] { ChunkIterator(-1, null) }
      assertThrows[IllegalArgumentException] { ChunkIterator(Int.MinValue, null) }
    }

  }

}
