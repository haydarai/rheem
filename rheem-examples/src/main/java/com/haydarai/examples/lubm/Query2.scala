package com.haydarai.examples.lubm

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

/**
 * # Query2
 * # This query increases in complexity: 3 classes and 3 properties are involved. Additionally,
 * # there is a triangular pattern of relationships between the objects involved.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X, ?Y, ?Z
 * WHERE
 * {?X rdf:type ub:GraduateStudent .
 * ?Y rdf:type ub:University .
 * ?Z rdf:type ub:Department .
 * ?X ub:memberOf ?Z .
 * ?Z ub:subOrganizationOf ?Y .
 * ?X ub:undergraduateDegreeFrom ?Y}
 */

object Query2 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 2")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X", rdf + "type", ub + "GraduateStudent"),
      Array("Y", rdf + "type", ub + "University"),
      Array("Z", rdf + "type", ub + "Department"),
      Array("X", ub + "memberOf", "Z"),
      Array("Z", ub + "subOrganizationOf", "Y"),
      Array("X", ub + "undergraduateDegreeFrom", "Y")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X", "Y", "Z")).withName("Project variables")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
