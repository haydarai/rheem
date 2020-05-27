package com.haydarai.examples.lubm.graph

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.api.graph.{Edge, Vertex}
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.basic.data.Tuple2
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource
import org.qcri.rheem.spark.Spark

import scala.collection.JavaConverters._

/**
 * # Query1
 * # This query bears large input and high selectivity. It queries about just one class and
 * # one property and does not assume any hierarchy information or inference.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X
 * WHERE
 * {?X rdf:type ub:GraduateStudent .
 * ?X ub:takesCourse
 * http://www.Department0.University0.edu/GraduateCourse0}
 */
object Query1Graph {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(RheemBasics.graphPlugin)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.channelConversionPlugin)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Spark.graphPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 1 with Graph Algorithm")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X", rdf + "type", ub + "GraduateStudent"),
      Array("X", ub + "takesCourse", "Y")
    )

    val projectedRecords = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X", "Y")).withName("Project variable X and Y")

    val edges = projectedRecords
      .flatMap(record => Seq((record.getField(0).toString, record.getField(1).toString)))

    val vertexIds = edges
      .flatMap(edge => Seq(edge._1, edge._2))
      .distinct
      .zipWithId

    type VertexId = Tuple2[Vertex, String]
    val idEdges = edges
      .join[VertexId, String](_._1, vertexIds, _.field1)
      .map { linkAndVertexId => (linkAndVertexId.field1.field0, linkAndVertexId.field0._2) }
      .join[VertexId, String](_._2, vertexIds, _.field1)
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))

    val pageRanks = idEdges.pageRank(10)

    val result = pageRanks
      .join[VertexId, Long](_.field0, vertexIds, _.field0)
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1))
      .collect()

    // Print query result
    result.foreach(println)
  }
}
