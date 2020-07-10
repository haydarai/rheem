package com.haydarai.examples.yago.jenaspark

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.data.{Record, Tuple2, Tuple3}
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource
import org.qcri.rheem.spark.Spark

import scala.collection.JavaConverters._

object Query3 {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Spark.graphPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("YAGO3: Query 3")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val y = "http://yago-knowledge.org/resource/"

    // Define triples definition
    val triplesA = List[Array[String]](
      Array("p1", y + "isMarriedTo", "p2")
    )

    // Define triples definition
    val triplesB = List[Array[String]](
      Array("p1", y + "wasBornIn", "c")
    )

    // Define triples definition
    val triplesC = List[Array[String]](
      Array("p2", y + "wasBornIn", "c")
    )

    val projectedP1P2 = planBuilder
      .readModel(new JenaModelSource(args(0), triplesA.asJava)).withName("Read RDF file")
      .projectRecords(List("p1", "p2")).withName("Project variable ?p1 ?p2")

    val projectedP1C = planBuilder
      .readModel(new JenaModelSource(args(0), triplesB.asJava)).withName("Read RDF file")
      .projectRecords(List("p1", "c")).withName("Project variable ?p1 ?c")

    val projectedP2C = planBuilder
      .readModel(new JenaModelSource(args(0), triplesC.asJava)).withName("Read RDF file")
      .projectRecords(List("p2", "c")).withName("Project variable ?p2 ?c")

    val P1P2C = projectedP1P2
      .join(r => r.getString(0),
        projectedP1C,
        (r: Record) => r.getString(0))
      .withTargetPlatforms(Jena.platform())

    val allJoined = P1P2C
      .join(t => t.field0.getString(1) + t.field1.getString(1),
        projectedP2C,
        (r: Record) => r.getString(0) + r.getString(1))
      .withTargetPlatforms(Jena.platform())

    val records = allJoined
      .map(t => new Tuple3(t.field0.field0.getString(0), t.field1.getString(0),
        t.field1.getString(1)))

    val edgesPageRank = records
      .flatMap(t => Seq((t.field0, t.field2), (t.field2, t.field0)))

    val vertexIds = records
      .flatMap(t => Seq(t.field0, t.field1, t.field2))
      .distinct
      .zipWithId

    // Assign id to edges
    type VertexId = Tuple2[Vertex, String]

    val idEdgesPageRank = edgesPageRank
      .join[VertexId, String](_._1, vertexIds, _.field1).withName("Assign ID to vertices (left) on edges")
      .map { linkAndVertexId => (linkAndVertexId.field1.field0, linkAndVertexId.field0._2) }
      .withName("Remove unnecessary fields from join results")

      .join[VertexId, String](_._2, vertexIds, _.field1).withName("Assign ID to vertices (right) on edges")
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))
      .withName("Remove unnecessary fields from join results")

    val pageRank = idEdgesPageRank.pageRank(10)
      .withTargetPlatforms(Spark.platform())

    val pageRankResults = pageRank
      .join[VertexId, Long](_.field0, vertexIds, _.field0)
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))

    val results = records
      .join[(String, java.lang.Float), String](_.field2, pageRankResults, _._1)
      .map(t2 => new Tuple3(t2.field0.field0, t2.field0.field1, t2.field1._2))
      .collect()

    results.foreach(t => println(t.toString))

    val endTime = System.currentTimeMillis()
    println(endTime - startTime)
  }
}
