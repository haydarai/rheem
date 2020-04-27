package com.haydarai.examples.graph

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.basic.data.Tuple2
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

object SingleSourceShortestPath {
  def main(args: Array[String]) {
    val inputUrl = "file:" + args(0)

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(RheemBasics.graphPlugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Spark.graphPlugin)
    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("PageRank")
      .withUdfJarsOf(this.getClass)

    val edges = planBuilder
      .readTextFile(inputUrl)
      .filter(!_.startsWith("#"))
      .withName("Filter comments")

      .map(parseTriple)
      .withName("Parse triples")

      .map { case (s, p, o) => (s, o) }
      .withName("Discard predicate")

    val vertexIds = edges
      .flatMap(edge => Seq(edge._1, edge._2))
      .withName("Extract vertices")

      .distinct
      .withName("Distinct vertices")

      .zipWithId
      .withName("Add vertex ids")

    type VertexId = Tuple2[Vertex, String]
    val idEdges = edges
      .join[VertexId, String](_._1, vertexIds, _.field1)
      .withName("Join source vertex IDs")

      .map { linkAndVertexId =>
        (linkAndVertexId.field1.field0, linkAndVertexId.field0._2)
      }
      .withName("Set source vertex ID")

      .join[VertexId, String](_._2, vertexIds, _.field1)
      .withName("Join target vertex IDs")

      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))
      .withName("Set target vertex ID")

    val singleSourceShortestPath = idEdges.singleSourceShortestPath(0)

    val result = singleSourceShortestPath
      .join[VertexId, Long](_.field0, vertexIds, _.field0)
      .withName("Join degree with vertex IDs")

      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))
      .withName("Make page ranks readable")

      .collect()

    print(result.take(10))
  }

  def parseTriple(raw: String): (String, String, String) = {
    // Find the first two spaces: Odds are that these are separate subject, predicated and object.
    val firstSpacePos = raw.indexOf(' ')
    val secondSpacePos = raw.indexOf(' ', firstSpacePos + 1)

    // Find the end position.
    var stopPos = raw.lastIndexOf('.')
    while (raw.charAt(stopPos - 1) == ' ') stopPos -= 1

    (raw.substring(0, firstSpacePos),
      raw.substring(firstSpacePos + 1, secondSpacePos),
      raw.substring(secondSpacePos + 1, stopPos))
  }
}
