package com.haydarai.examples.spartex.jenaspark

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.basic.data.{Record, Tuple2, Tuple3}
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource
import org.qcri.rheem.spark.Spark

import scala.collection.JavaConverters._

object Query1 {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(RheemBasics.graphPlugin)
      .withPlugin(Jena.plugin)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Spark.graphPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("Spartex: Query 1 on Java")
      .withUdfJarsOf(this.getClass)

    val triplesA = List[Array[String]](
      Array("p", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf", "c")
    )

    val triplesB = List[Array[String]](
      Array("s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse", "c")
    )

    val triplesC = List[Array[String]](
      Array("s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#advisor", "p")
    )


    // Read RDF file and project selected variables
    val projectedRecordsPC = planBuilder
      .readModel(new JenaModelSource(args(0), triplesA.asJava)).withName("Read RDF file A")
      .projectRecords(List("p", "c")).withName("Project triple A")

    val projectedRecordsSC = planBuilder
      .readModel(new JenaModelSource(args(0), triplesB.asJava)).withName("Read RDF file B")
      .projectRecords(List("s", "c")).withName("Project triple B")

    val projectedRecordsSP = planBuilder
      .readModel(new JenaModelSource(args(0), triplesC.asJava)).withName("Read RDF file C")
      .projectRecords(List("s", "p")).withName("Project triple C")

    val joinedSCPC = projectedRecordsSC
      .join((r: Record) => r.getString(1),
        projectedRecordsPC,
        (r: Record) => r.getString(1)
      )
      .withTargetPlatforms(Jena.platform())
      .withName("Join 1")

    val allJoined = joinedSCPC
      .join((t: Tuple2[Record, Record]) => t.getField0.getString(0) + t.getField1.getString(0),
        projectedRecordsSP,
        (r: Record) => r.getString(0) + r.getString(1))
      .withTargetPlatforms(Jena.platform())
      .withName("Join 2")

    val records = allJoined.map(record => new Tuple3(record.getField0.getField0.getString(0),
      record.getField0.getField1.getString(0), record.getField0.getField0.getString(0)))

    // Create list of edges that will be passed to PageRank algorithm (?s ?p and ?p ?c)
    val edgesPageRank = records
      .flatMap(record => Seq((record.field0, record.field1), (record.field1, record.field2)))
      .withName("Generate edges for PageRank algorithm")

    // Create list of edges that will be passed to DegreeCentrality algorithm (?s ?c and ?p ?c)
    val edgesCentrality = records
      .flatMap(record => Seq((record.field0, record.field2), (record.field1, record.field2)))
      .withName("Generate edges for DegreeCentrality algorithm")

    val vertexIds = records
      .flatMap(record => Seq(record.field0, record.field1, record.field2))
      .withName("List all vertices")

      .distinct
      .withName("Remove duplicates")

      .zipWithId
      .withName("Create ID for each individual vertex")

    // Assign id to edges
    type VertexId = Tuple2[Vertex, String]

    val idEdgesPageRank = edgesPageRank
      .join[VertexId, String](_._1, vertexIds, _.field1).withName("Assign ID to vertices (left) on edges")
      .map { linkAndVertexId => (linkAndVertexId.field1.field0, linkAndVertexId.field0._2) }
      .withName("Remove unnecessary fields from join results")

      .join[VertexId, String](_._2, vertexIds, _.field1).withName("Assign ID to vertices (right) on edges")
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))
      .withName("Remove unnecessary fields from join results")

    val idEdgesDegreeCentrality = edgesCentrality
      .join[VertexId, String](_._1, vertexIds, _.field1).withName("Assign ID to vertices (left) on edges")
      .map { linkAndVertexId => (linkAndVertexId.field1.field0, linkAndVertexId.field0._2) }
      .withName("Remove unnecessary fields from join results")

      .join[VertexId, String](_._2, vertexIds, _.field1).withName("Assign ID to vertices (right) on edges")
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))
      .withName("Remove unnecessary fields from join results")

    // Run graph algorithms
    val pageRanks = idEdgesPageRank.pageRank(10)
      .withTargetPlatforms(Spark.platform())
    val degreeCentrality = idEdgesDegreeCentrality.degreeCentrality()
      .withTargetPlatforms(Spark.platform())

    // Convert algorithm results with vertex ID to the actual string
    val pageRankResults = pageRanks
      .join[VertexId, Long](_.field0, vertexIds, _.field0).withName("Join back with vertex ID list")
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))
      .withName("Get the actual string representation and discard the vertex ID")

    val degreeCentralityResults = degreeCentrality
      .join[VertexId, Long](_.field0, vertexIds, _.field0).withName("Join back with vertex ID list")
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))
      .withName("Get the actual string representation and discard the vertex ID")

    // Join algorithm results to records, then apply filter based on the algorithm results
    val results = records
      .map(record => new Tuple3(record.field0, record.field1, record.field2))
      .withName("Get all relevant variables ?s ?p ?c")

      .join[(String, java.lang.Float), String](_.field1, pageRankResults, _._1)
      .withName("Join it with PageRank results")

      .map(tuple2 => new Tuple3(tuple2.field0.field0, tuple2.field1._2, tuple2.field0.field2))
      .withName("Discard the variable string and keeping the PageRank value")

      .join[(String, Integer), String](_.field2, degreeCentralityResults, _._1)
      .withName("Join it with DegreeCentrality results")

      .map(tuple2 => new Tuple3(tuple2.field0.field0, tuple2.field0.field1, tuple2.field1._2))
      .withName("Discard the variable string and keeping the DegreeCentrality value")

      .filter(tuple3 => tuple3.field1 > 0 && tuple3.field2 > 1)
      .withName("Filter based on PageRank and DegreeCentrality value")

      .map(tuple3 => (tuple3.field0)).withName("Get only projected variable ?s")

      .collect()

    // Print query result
    results.foreach(println)

    val endTime = System.currentTimeMillis()
    println(endTime - startTime)
  }
}
