package com.haydarai.examples.yago.java8

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.data.{Record, Tuple2}
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

object Query1 {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.graphPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("YAGO3: Query 1")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val y = "http://yago-knowledge.org/resource/"

    // Define triples definition
    val triplesA = List[Array[String]](
      Array("p", y + "wasBornIn", "c")
    )

    // Define triples definition
    val triplesB = List[Array[String]](
      Array("p", y + "hasAcademicAdvisor", "a")
    )

    // Define triples definition
    val triplesC = List[Array[String]](
      Array("a", y + "wasBornIn", "c")
    )

    val projectedPC = planBuilder
      .readModel(new JenaModelSource(args(0), triplesA.asJava)).withName("Read RDF file")
      .projectRecords(List("p", "c")).withName("Project variable ?p ?c")

    val projectedPA = planBuilder
      .readModel(new JenaModelSource(args(0), triplesB.asJava)).withName("Read RDF file")
      .projectRecords(List("p", "a")).withName("Project variable ?p ?a")

    val projectedAC = planBuilder
      .readModel(new JenaModelSource(args(0), triplesC.asJava)).withName("Read RDF file")
      .projectRecords(List("a", "c")).withName("Project variable ?a ?c")

    val PPCA = projectedPC
      .join(r => r.getString(0),
        projectedPA,
        (r: Record) => r.getString(0))
      .withTargetPlatforms(Java.platform())

    val allJoined = PPCA
      .join(t => t.getField0.getString(1) + t.getField1.getString(1),
        projectedAC,
        (r: Record) => r.getString(1) + r.getString(0))
      .withTargetPlatforms(Java.platform())
      .filter(t => !t.field1.getString(1).equalsIgnoreCase("http://yago-knowledge.org/resource/Melbourne"))
      .withTargetPlatforms(Java.platform())

    val records = allJoined
      .map(t => Tuple3(t.field0.field0.getString(0), t.field1.getString(0), t.field1.getString(1)))

    val edgesCentrality = records
      .flatMap(t => Seq((t._1, t._2)))
      .withName("Generate edges for Degree Centrality algorithm")

    val edgesPageRank = records
      .flatMap(t => Seq((t._1, t._3), (t._2, t._3)))
      .withName("Generate edges for Page Rank algorithm")

    val vertexIds = records
      .flatMap(t => Seq(t._1, t._2, t._3))
      .distinct
      .zipWithId

    // Assign id to edges
    type VertexId = Tuple2[Vertex, String]

    val idEdgesCentrality = edgesCentrality
      .join[VertexId, String](_._1, vertexIds, _.field1).withName("Assign ID to vertices (left) on edges")
      .map { linkAndVertexId => (linkAndVertexId.field1.field0, linkAndVertexId.field0._2) }
      .withName("Remove unnecessary fields from join results")

      .join[VertexId, String](_._2, vertexIds, _.field1).withName("Assign ID to vertices (right) on edges")
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))
      .withName("Remove unnecessary fields from join results")

    val idEdgesPageRank = edgesPageRank
      .join[VertexId, String](_._1, vertexIds, _.field1).withName("Assign ID to vertices (left) on edges")
      .map { linkAndVertexId => (linkAndVertexId.field1.field0, linkAndVertexId.field0._2) }
      .withName("Remove unnecessary fields from join results")

      .join[VertexId, String](_._2, vertexIds, _.field1).withName("Assign ID to vertices (right) on edges")
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))
      .withName("Remove unnecessary fields from join results")

    val degreeCentrality = idEdgesCentrality.degreeCentrality()
      .withTargetPlatforms(Java.platform())
    val pageRanks = idEdgesPageRank.pageRank(10)
      .withTargetPlatforms(Java.platform())

    val degreeCentralityResults = degreeCentrality
      .join[VertexId, Long](_.field0, vertexIds, _.field0)
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))

    val pageRankResults = pageRanks
      .join[VertexId, Long](_.field0, vertexIds, _.field0)
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))

    val results = records
      .join[(String, java.lang.Integer), String](_._2, degreeCentralityResults, _._1)
      .map(t2 => Tuple3(t2.field0._1, t2.field1._2, t2.field0._3))
      .join[(String, java.lang.Float), String](_._3, pageRankResults, _._1)
      .map(t2 => Tuple3(t2.field0._1, t2.field0._2, t2.field1._2))
      .collect()

    // Print query result
    println(results)

    val endTime = System.currentTimeMillis()
    println(endTime - startTime)
  }
}
