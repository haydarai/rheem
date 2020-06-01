package com.haydarai.examples.spartex

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.basic.data.{Tuple2, Tuple3}
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource
import org.qcri.rheem.spark.Spark

import scala.collection.JavaConverters._

object SparkQuery1 {
  def main(args: Array[String]) {

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(RheemBasics.graphPlugin)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Spark.graphPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("Spartex: Query 1")
      .withUdfJarsOf(this.getClass)

    // Define triples definition
    val triples = List[Array[String]](
      Array("p", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf", "c"),
      Array("s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse", "c"),
      Array("s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#advisor", "p")
    )

    // Read RDF file and project selected variables
    val projectedRecords = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .map(record => (record.getField(2), record.getField(0), record.getField(1)))
      .withName("Project variables defined in triple definitions")

    // Create list of edges that will be passed to PageRank algorithm (?s ?p and ?p ?c)
    val edgesPageRank = projectedRecords
      .flatMap(record => Seq((record._1.toString, record._2.toString),
        (record._2.toString, record._3.toString)))
      .withName("Generate edges for PageRank algorithm")

    // Create list of edges that will be passed to DegreeCentrality algorithm (?s ?c and ?p ?c)
    val edgesCentrality = projectedRecords
      .flatMap(record => Seq((record._1.toString, record._3.toString),
        (record._2.toString, record._3.toString)))
      .withName("Generate edges for DegreeCentrality algorithm")

    val vertexIds = projectedRecords
      .flatMap(record => Seq(record._1.toString, record._2.toString,
        record._3.toString))
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
    val degreeCentrality = idEdgesDegreeCentrality.degreeCentrality()

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
    val results = projectedRecords
      .map(record => new Tuple3(record._1.toString, record._2.toString, record._3.toString))
      .withName("Get all relevant variables ?s ?p ?c")

      .join[(String, java.lang.Float), String](_.field1, pageRankResults, _._1)
      .withName("Join it with PageRank results")

      .map(tuple2 => new Tuple3(tuple2.field0.field0, tuple2.field1._2, tuple2.field0.field2))
      .withName("Discard the variable string and keeping the PageRank value")

      .join[(String, Integer), String](_.field2, degreeCentralityResults, _._1)
      .withName("Join it with DegreeCentrality results")

      .map(tuple2 => new Tuple3(tuple2.field0.field0, tuple2.field0.field1, tuple2.field1._2))
      .withName("Discard the variable string and keeping the DegreeCentrality value")

      .filter(tuple3 => tuple3.field1 > 0.3 && tuple3.field2 > 1)
      .withName("Filter based on PageRank and DegreeCentrality value")

      .map(tuple3 => (tuple3.field0)).withName("Get only projected variable ?s")

      .collect()

    // Print query result
    results.foreach(println)
  }
}