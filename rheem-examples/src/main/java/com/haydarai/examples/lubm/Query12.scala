package com.haydarai.examples.lubm

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

/**
 * # Query12
 * # The benchmark data do not produce any instances of class Chair. Instead, each
 * # Department individual is linked to the chair professor of that department by
 * # property headOf. Hence this query requires realization, i.e., inference that
 * # that professor is an instance of class Chair because he or she is the head of a
 * # department. Input of this query is small as well.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X, ?Y
 * WHERE
 * {?X rdf:type ub:Chair .
 * ?Y rdf:type ub:Department .
 * ?X ub:worksFor ?Y .
 * ?Y ub:subOrganizationOf <http://www.University0.edu>}
 */
object Query12 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 12")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X", rdf + "type", ub + "FullProfessor"),
      Array("Y", rdf + "type", ub + "Department"),
      Array("X", ub + "worksFor", "Y"),
      Array("Y", ub + "subOrganizationOf", "http://www.University0.edu")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X", "Y")).withName("Project variables")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
