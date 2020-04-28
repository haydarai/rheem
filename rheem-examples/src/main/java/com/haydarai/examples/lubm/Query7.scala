package com.haydarai.examples.lubm

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

/**
 * # Query7
 * # This query is similar to Query 6 in terms of class Student but it increases in the
 * # number of classes and properties and its selectivity is high.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X, ?Y
 * WHERE
 * {?X rdf:type ub:Student .
 * ?Y rdf:type ub:Course .
 * ?X ub:takesCourse ?Y .
 * <http://www.Department0.University0.edu/AssociateProfessor0>,
 * ub:teacherOf, ?Y}
 */
object Query7 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 7")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X", rdf + "type", ub + "UndergraduateStudent"),
      Array("Y", rdf + "type", ub + "Course"),
      Array("X", ub + "takesCourse", "Y")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X", "Y")).withName("Project variables")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
