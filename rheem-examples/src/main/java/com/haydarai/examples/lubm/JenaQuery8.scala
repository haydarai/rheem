package com.haydarai.examples.lubm

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

/**
 * # Query8
 * # This query is further more complex than Query 7 by including one more property.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X, ?Y, ?Z
 * WHERE
 * {?X rdf:type ub:Student .
 * ?Y rdf:type ub:Department .
 * ?X ub:memberOf ?Y .
 * ?Y ub:subOrganizationOf <http://www.University0.edu> .
 * ?X ub:emailAddress ?Z}
 */
object JenaQuery8 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 8")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X", rdf + "type", ub + "UndergraduateStudent"),
      Array("Y", rdf + "type", ub + "Department"),
      Array("X", ub + "memberOf", "Y"),
      Array("Y", ub + "subOrganizationOf", "http://www.University0.edu"),
      Array("X", ub + "emailAddress", "Z")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X", "Y", "Z")).withName("Project variables")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
