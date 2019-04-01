package org.qcri.rheem.apps.simwords

import java.util

import org.qcri.rheem.basic.data.Tuple2
import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.rest.TupleSparse

import scala.collection.JavaConversions._

/**
  * This function creates word neighborhood vectors out of a text.
  */
class CreateWordNeighborhoodRestFunction(neighborhoodReach: Int, dictionaryBroadcastName: String)
  extends ExtendedSerializableFunction[String, java.lang.Iterable[TupleSparse]] {

  private var dictionary: Map[String, Int] = _

  private lazy val textScrubber = new TextScrubber

  private lazy val collector = new java.util.ArrayList[String]

  override def open(ctx: ExecutionContext): Unit = {
    this.dictionary = ctx.getBroadcast[Tuple2[String, Int]](dictionaryBroadcastName).map(s => (s.field0, s.field1)).toMap
  }

  override def apply(value: String): java.lang.Iterable[TupleSparse] = {
    val result = new util.LinkedList[TupleSparse]()

    this.textScrubber.splitAndScrub(value, this.collector)
    // Make sure that there is at least one neighbor; otherwise, the resulting vector will not support cosine similarity
    if (this.collector.size > 1) {
      val wordIds = this.collector.map(this.dictionary.getOrElse(_, -1))
      for (i <- wordIds.indices) {
        val builder = new SparseVector.Builder
        for (j <- math.max(0, i - neighborhoodReach) until i; if wordIds(j) != -1) {
          builder.add(wordIds(j), 1)
        }
        for (j <- i + 1 until math.min(wordIds.size, i + neighborhoodReach + 1); if wordIds(j) != -1) {
          builder.add(wordIds(j), 1)
        }
        if (!builder.isEmpty) result.add(new TupleSparse(wordIds(i), builder.build))
      }
      this.collector.clear()
    }

    result
  }
}
