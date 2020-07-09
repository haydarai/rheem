package com.haydarai.examples.spartex.java

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.basic.data.{Record, Tuple2, Tuple3}
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
      .withPlugin(RheemBasics.graphPlugin)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.graphPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("Spartex: Query 2")
      .withUdfJarsOf(this.getClass)

    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    val triplesA = List[Array[String]](
      Array("X", rdf + "type", ub + "GraduateStudent")
    )

    val triplesB = List[Array[String]](
      Array("Y", rdf + "type", ub + "University")
    )

    val triplesC = List[Array[String]](
      Array("Z", rdf + "type", ub + "Department")
    )

    val triplesD = List[Array[String]](
      Array("X", ub + "memberOf", "Z")
    )

    val triplesE = List[Array[String]](
      Array("Z", ub + "subOrganizationOf", "Y")
    )

    // Read RDF file and project selected variables
    val projectedRecordsX = planBuilder
      .readModel(new JenaModelSource(args(0), triplesA.asJava)).withName("Read RDF file A")
      .projectRecords(List("X")).withName("Project triple X")

    val projectedRecordsY = planBuilder
      .readModel(new JenaModelSource(args(0), triplesB.asJava)).withName("Read RDF file B")
      .projectRecords(List("Y")).withName("Project triple Y")

    val projectedRecordsZ = planBuilder
      .readModel(new JenaModelSource(args(0), triplesC.asJava)).withName("Read RDF file C")
      .projectRecords(List("Z")).withName("Project triple Z")

    val projectedRecordsXZ = planBuilder
      .readModel(new JenaModelSource(args(0), triplesD.asJava)).withName("Read RDF file D")
      .projectRecords(List("X", "Z")).withName("Project triple ZY")

    val projectedRecordsZY = planBuilder
      .readModel(new JenaModelSource(args(0), triplesE.asJava)).withName("Read RDF file D")
      .projectRecords(List("Z", "Y")).withName("Project triple ZY")

    val joinXZ = projectedRecordsZ
      .join((r: Record) => r.getString(0),
        projectedRecordsXZ,
        (r: Record) => r.getString(1))
      .withTargetPlatforms(Java.platform())

    val joinXYZ = joinXZ
      .join((t: Tuple2[Record, Record]) => t.getField0.getString(0),
        projectedRecordsZY,
        (r: Record) => r.getString(0)
      )
      .withTargetPlatforms(Java.platform())

    val joinXXYZ = joinXYZ
      .join(t => t.getField0.getField1.getString(0),
        projectedRecordsX,
        (r: Record) => r.getString(0)
      )
      .withTargetPlatforms(Java.platform())

    val allJoined = joinXXYZ
      .join(t => t.getField0.getField1.getString(1),
        projectedRecordsY,
        (r: Record) => r.getString(0)
      )
      .withTargetPlatforms(Java.platform())

    // Valid for Spark and Java
    val records = allJoined.map(record => new Tuple3(record.getField0.getField0.getField0.getField1.getString(0),
      record.getField0.getField0.getField1.getString(1), record.getField0.getField0.getField0.getField1.getString(1)))

    // Create list of edges that will be passed to SingleSourceShortestPath algorithm (?x ?y and ?y ?x)
    val edgesShortestPath = records
      .flatMap(record => Seq((record.field0, record.field2), (record.field2, record.field1), (record.field1, record.field2), (record.field2, record.field0)))
      .withName("Generate edges for PageRank algorithm")

    // Create list of edges that will be passed to DegreeCentrality algorithm (?x ?y)
    val edgesCentrality = records
      .flatMap(record => Seq((record.field0, record.field2)))
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

    val idEdgesShortestPath = edgesShortestPath
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
    val shortestPath = idEdgesShortestPath.singleSourceShortestPath(0)
      .withTargetPlatforms(Java.platform())
    val degreeCentrality = idEdgesDegreeCentrality.degreeCentrality()
      .withTargetPlatforms(Java.platform())

    // Convert algorithm results with vertex ID to the actual string
    val shortestPathResults = shortestPath
      .join[VertexId, Long](_.field0, vertexIds, _.field0).withName("Join back with vertex ID list")
      .map(joinTuple => new Tuple2(joinTuple.field1.field1, joinTuple.field0.field1))
      .withName("Get the actual string representation and discard the vertex ID")

    val degreeCentralityResults = degreeCentrality
      .join[VertexId, Long](_.field0, vertexIds, _.field0).withName("Join back with vertex ID list")
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))
      .withName("Get the actual string representation and discard the vertex ID")

    // Join algorithm results to records, then apply filter based on the algorithm results
    val results = records
      .map(record => new Tuple3(record.field0, record.field1, record.field2))
      .withName("Get all relevant variables ?s ?p ?c")
      .join(t => t.field0, shortestPathResults, (t: Tuple2[String, java.lang.Float]) => t.field0)
      .map(tuple2 => new Tuple3(tuple2.field0.field0, tuple2.field1.field1, tuple2.field0.field2))
      .withName("Join it with Single Source Shortest Path results")
      .join[(String, Integer), String](_.field2, degreeCentralityResults, _._1)
      .withName("Join it with DegreeCentrality results")
      .map(tuple2 => new Tuple3(tuple2.field0.field0, tuple2.field0.field1, tuple2.field1._2))
      .withName("Discard the variable string and keeping the DegreeCentrality value")

//      .map(tuple3 => (tuple3.field0)).withName("Get only projected variable ?s")

      .collect()

    // Print query result
    results.foreach(println)

    val endTime = System.currentTimeMillis()
    println(endTime - startTime)
  }
}
