package com.haydarai.examples.spartex

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

object SpartexQuery3 {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(RheemBasics.graphPlugin)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.graphPlugin)
      .withPlugin(Java.channelConversionPlugin)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Spark.graphPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("Spartex: Query 3")
      .withUdfJarsOf(this.getClass)

    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    val triplesA = List[Array[String]](
      Array("s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#memberOf", "d")
    )

    val triplesB = List[Array[String]](
      Array("d", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#subOrganizationOf", "u")
    )

    val triplesC = List[Array[String]](
      Array("s", rdf + "type", "t")
    )

    // Read RDF file and project selected variables
    val projectedRecordsSD = planBuilder
      .readModel(new JenaModelSource(args(0), triplesA.asJava)).withName("Read RDF file A")
      .projectRecords(List("s", "d")).withName("Project ?s ?d")

    val projectedRecordsDU = planBuilder
      .readModel(new JenaModelSource(args(0), triplesB.asJava)).withName("Read RDF file B")
      .projectRecords(List("d", "u")).withName("Project ?d ?u")

    val projectedRecordsS = planBuilder
      .readModel(new JenaModelSource(args(0), triplesC.asJava)).withName("Read RDF file C")
      .projectRecords(List("s", "t")).withName("Project ?s ?t")

    val SDU = projectedRecordsSD
      .join((r: Record) => r.getString(1),
        projectedRecordsDU,
        (r: Record) => r.getString(0)
      )
      .withTargetPlatforms(Java.platform())
      .withName("Join on ?d")

    val all = SDU
      .join(r => r.field0.getString(0),
        projectedRecordsS,
        (r: Record) => r.getString(0))
      .withTargetPlatforms(Java.platform())

    val filtered = all
      .filter(t => !t.getField1.getString(1)
        .equalsIgnoreCase("http://swat.cse.lehigh.edu/onto/univ-bench.owl#UndergraduateStudent"),
        "t != 'http://swat.cse.lehigh.edu/onto/univ-bench.owl#UndergraduateStudent'")
      .withTargetPlatforms(Java.platform())

    // Valid for Jena
//    val records = filtered.map(record => new Tuple3(record.getField0.getField0.getString(0),
//      record.getField0.getField1.getString(0), record.getField0.getField1.getString(1)))

    // Valid for Spark and Java
    val records = filtered.map(record => new Tuple3(record.getField0.getField0.getString(0),
      record.getField0.getField0.getString(1), record.getField0.getField1.getString(1)))

    // Create list of edges that will be passed to PageRank algorithm (?s ?p and ?p ?c)
    val edgesPageRank = records
      .flatMap(record => Seq((record.field0, record.field1), (record.field1, record.field2),
        (record.field2, record.field1), (record.field1, record.field0)))
      .withName("Generate edges for PageRank algorithm")

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

    // Run graph algorithms
    val pageRanks = idEdgesPageRank.pageRank(10)
      .withTargetPlatforms(Spark.platform())

    // Convert algorithm results with vertex ID to the actual string
    val pageRankResults = pageRanks
      .join[VertexId, Long](_.field0, vertexIds, _.field0).withName("Join back with vertex ID list")
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))
      .withName("Get the actual string representation and discard the vertex ID")

    // Join algorithm results to records, then apply filter based on the algorithm results
    val results = records
      .map(record => new Tuple3(record.field0, record.field1, record.field2))
      .withName("Get all relevant variables ?s ?p ?c")

      .join[(String, java.lang.Float), String](_.field2, pageRankResults, _._1)
      .withName("Join it with PageRank results")

      .map(tuple2 => new Tuple2(tuple2.field0.field0, tuple2.field1._2))
      .withName("Discard the variable string and keeping the PageRank value")
      .collect()

    // Print query result
    results.foreach(println)

    val endTime = System.currentTimeMillis()
    println(endTime - startTime)
  }
}
