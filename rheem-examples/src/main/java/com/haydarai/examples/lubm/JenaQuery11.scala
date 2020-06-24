package com.haydarai.examples.lubm

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

/**
 * # Query11
 * # Query 11, 12 and 13 are intended to verify the presence of certain OWL reasoning
 * # capabilities in the system. In this query, property subOrganizationOf is defined
 * # as transitive. Since in the benchmark data, instances of ResearchGroup are stated
 * # as a sub-organization of a Department individual and the later suborganization of
 * # a University individual, inference about the subOrgnizationOf relationship between
 * # instances of ResearchGroup and University is required to answer this query.
 * # Additionally, its input is small.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X
 * WHERE
 * {?X rdf:type ub:ResearchGroup .
 * ?X ub:subOrganizationOf <http://www.University0.edu>}
 */
object JenaQuery11 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 11")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X", rdf + "type", ub + "Department"),
      Array("X", ub + "subOrganizationOf", "http://www.University0.edu")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X")).withName("Project variable ?X")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
