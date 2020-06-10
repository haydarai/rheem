package com.haydarai.examples.lubm.graph

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.basic.data.Tuple2
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource
import org.qcri.rheem.spark.Spark

import scala.collection.JavaConverters._

/**
 * # Query6
 * # This query queries about only one class. But it assumes both the explicit
 * # subClassOf relationship between UndergraduateStudent and Student and the
 * # implicit one between GraduateStudent and Student. In addition, it has large
 * # input and low selectivity.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X WHERE {?X rdf:type ub:Student}
 */
object Query6Graph {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(RheemBasics.graphPlugin)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.channelConversionPlugin)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Spark.graphPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 3")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X1", rdf + "type", ub + "UndergraduateStudent"),
      Array("X2", rdf + "type", ub + "UndergraduateStudent"),
      Array("X1", ub + "memberOf", "Y"),
      Array("X2", ub + "memberOf", "Y")
    )

    val projectedRecords = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X1", "X2")).withName("Project variables")

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

    val degreeCentrality = idEdges.singleSourceShortestPath(0);

    val results = degreeCentrality
      .join[VertexId, Long](_.field0, vertexIds, _.field0)
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1.toInt))
      .filter(record => record._2 < 2)
      .collect()

    // Print query result
    results.foreach(println)
  }
}