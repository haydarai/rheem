package org.qcri.rheem.apps.simwords

import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.rest.TupleSparse
import org.qcri.rheem.basic.data.Tuple2
import scala.collection.JavaConversions._

class ExtendWordVectorRest(broadcastName: String)
  extends ExtendedSerializableFunction[TupleSparse,  String] {

  private var words: Map[Int, String] = _

  /**
    * Called before this instance is actually executed.
    *
    * @param ctx the { @link ExecutionContext}
    */
  override def open(ctx: ExecutionContext): Unit = {
    this.words = ctx.getBroadcast[Tuple2[String, Int]](broadcastName).map(s => (s.field1, s.field0)).toMap
  }

  override def apply(t: TupleSparse): String = {
    var wv = (t.field0, this.words.getOrElse(t.field0, "(unknown)"), t.field1)
    return s"${wv._1};${wv._2};${wv._3.toDictionaryString}"
  }

}
