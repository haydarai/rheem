package com.haydarai.examples.lubm

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

/**
 * # Query4
 * # This query has small input and high selectivity. It assumes subClassOf relationship
 * # between Professor and its subclasses. Class Professor has a wide hierarchy. Another
 * # feature is that it queries about multiple properties of a single class.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X, ?Y1, ?Y2, ?Y3
 * WHERE
 * {?X rdf:type ub:Professor .
 * ?X ub:worksFor <http://www.Department0.University0.edu> .
 * ?X ub:name ?Y1 .
 * ?X ub:emailAddress ?Y2 .
 * ?X ub:telephone ?Y3}
 */
object JenaQuery4 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 4")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X", rdf + "type", ub + "FullProfessor"),
      Array("X", ub + "worksFor", "http://www.Department0.University0.edu"),
      Array("X", ub + "name", "Y1"),
      Array("X", ub + "emailAddress", "Y2"),
      Array("X", ub + "telephone", "Y3")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X", "Y1", "Y2", "Y3")).withName("Project variables")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
