package com.haydarai.examples.spartex

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.basic.data.{Tuple2, Tuple3, Tuple4}
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

object SparkQuery1 {
  def main(args: Array[String]) {

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(RheemBasics.graphPlugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Spark.graphPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("Spartex: Query 1")
      .withUdfJarsOf(this.getClass)

    // Read RDF file and project selected variables
    val projectedRecordsPC = planBuilder
      .readTextFile("file:" + args(0))
      .map(parseTriple)
      .filter(_.field1.equals("http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf"))
      .map(record => new Tuple2(record.field0, record.field2))

    val projectedRecordsSC = planBuilder
      .readTextFile("file:" + args(0))
      .map(parseTriple)
      .filter(_.field1.equals("http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse"))
      .map(record => new Tuple2(record.field0, record.field2))

    val projectedRecordsSP = planBuilder
      .readTextFile("file:" + args(0))
      .map(parseTriple)
      .filter(_.field1.equals("http://swat.cse.lehigh.edu/onto/univ-bench.owl#advisor"))
      .map(record => new Tuple4(record.field0, record.field2, "", record.field0 + record.field2))

    val joinedSCPC = projectedRecordsSC
      .join[Tuple2[String, String], String](_.getField1, projectedRecordsPC, _.getField1)
      .map(tuple => new Tuple4(tuple.field0.field0, tuple.field0.field1, tuple.field1.field1,
        tuple.field0.field0 + tuple.field1.field0))

    val records = joinedSCPC
      .join[Tuple4[String, String, String, String], String](_.getField3, projectedRecordsSP, _.getField3)
      .map(tuple => new Tuple3(tuple.field0.field0, tuple.field0.field1, tuple.field0.field2))

    // Create list of edges that will be passed to PageRank algorithm (?s ?p and ?p ?c)
    val edgesPageRank = records
      .flatMap(record => Seq((record.field0, record.field1), (record.field1, record.field2)))
      .withName("Generate edges for PageRank algorithm")

    // Create list of edges that will be passed to DegreeCentrality algorithm (?s ?c and ?p ?c)
    val edgesCentrality = records
      .flatMap(record => Seq((record.field0, record.field2), (record.field2, record.field2)))
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

      .filter(tuple3 => tuple3.field1 > 0.3 && tuple3.field2 > 1)
      .withName("Filter based on PageRank and DegreeCentrality value")

      .map(tuple3 => (tuple3.field0)).withName("Get only projected variable ?s")

      .collect()

    // Print query result
    results.foreach(println)
  }

  def parseTriple(raw: String): Tuple3[String, String, String] = {
    // Find the first two spaces: Odds are that these are separate subject, predicated and object.
    val firstSpacePos = raw.indexOf(' ')
    val secondSpacePos = raw.indexOf(' ', firstSpacePos + 1)

    // Find the end position.
    var stopPos = raw.lastIndexOf('.')
    while (raw.charAt(stopPos - 1) == ' ') stopPos -= 1

    new Tuple3(raw.substring(1, firstSpacePos - 1), raw.substring(firstSpacePos + 2, secondSpacePos - 1),
      raw.substring(secondSpacePos + 2, stopPos - 1))
  }
}