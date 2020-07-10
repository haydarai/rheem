package com.haydarai.examples.yago.jenajava

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.data.{Record, Tuple2, Tuple3, Tuple4}
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

object Query2 {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.graphPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("YAGO3: Query 2")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val y = "http://yago-knowledge.org/resource/"

    // Define triples definition
    val triplesA = List[Array[String]](
      Array("a1", y + "actedIn", "m")
    )

    // Define triples definition
    val triplesB = List[Array[String]](
      Array("a2", y + "actedIn", "m")
    )

    val projectedA1 = planBuilder
      .readModel(new JenaModelSource(args(0), triplesA.asJava)).withName("Read RDF file")
      .projectRecords(List("a1", "m")).withName("Project variable ?a1 ?m")

    val projectedA2 = planBuilder
      .readModel(new JenaModelSource(args(0), triplesB.asJava)).withName("Read RDF file")
      .projectRecords(List("a2", "m")).withName("Project variable ?a2 ?m")

    val allJoined = projectedA1
      .join(r => r.getString(1),
        projectedA2,
        (r: Record) => r.getString(1))
      .withTargetPlatforms(Jena.platform)

    val records = allJoined
      .map(t => new Tuple3(t.field0.getString(0), t.getField1.getString(0),
        t.getField0.getString(1)))

    val edgesSingleSource = records
      .flatMap(t => Seq((t.field0, t.field2), (t.field2, t.field0)))
      .withName("Generate edges for Single Source Shortest Path algorithm")

    val edgesCentrality = records
      .flatMap(t => Seq((t.field0, t.field2), (t.field2, t.field0)))
      .withName("Generate edges for Degree Centrality algorithm")

    val vertexIds = records
      .flatMap(t => Seq(t.field0, t.field1, t.field2))
      .distinct
      .zipWithId

    // Assign id to edges
    type VertexId = Tuple2[Vertex, String]

    val idEdgesSingleSource = edgesSingleSource
      .join[VertexId, String](_._1, vertexIds, _.field1).withName("Assign ID to vertices (left) on edges")
      .map { linkAndVertexId => (linkAndVertexId.field1.field0, linkAndVertexId.field0._2) }
      .withName("Remove unnecessary fields from join results")

      .join[VertexId, String](_._2, vertexIds, _.field1).withName("Assign ID to vertices (right) on edges")
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))
      .withName("Remove unnecessary fields from join results")

    val idEdgesCentrality = edgesCentrality
      .join[VertexId, String](_._1, vertexIds, _.field1).withName("Assign ID to vertices (left) on edges")
      .map { linkAndVertexId => (linkAndVertexId.field1.field0, linkAndVertexId.field0._2) }
      .withName("Remove unnecessary fields from join results")

      .join[VertexId, String](_._2, vertexIds, _.field1).withName("Assign ID to vertices (right) on edges")
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))
      .withName("Remove unnecessary fields from join results")

    val singleSourceShortestPath = idEdgesSingleSource.singleSourceShortestPath(0)
      .withTargetPlatforms(Java.platform())
    val degreeCentrality = idEdgesCentrality.degreeCentrality()
      .withTargetPlatforms(Java.platform())

    val singleSourceShortestPathResults = singleSourceShortestPath
      .join[VertexId, Long](_.field0, vertexIds, _.field0)
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))

    val degreeCentralityResults = degreeCentrality
      .join[VertexId, Long](_.field0, vertexIds, _.field0)
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))

    val results = records
      .join[(String, java.lang.Float), String](_.field0, singleSourceShortestPathResults, _._1)
      .map(t2 => new Tuple4(t2.field0.field0, t2.field0.field1, t2.field1._2, t2.field0.field2))
      .join[(String, java.lang.Integer), String](_.field3, degreeCentralityResults, _._1)
      .map(t2 => new Tuple4(t2.field0.field0, t2.field0.field1, t2.field0.field2, t2.field1._2))
      .collect()

    results.foreach(t => println(t.toString))

    val endTime = System.currentTimeMillis()
    println(endTime - startTime)
  }
}
