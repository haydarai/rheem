package com.haydarai.examples.lubm

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

/**
 * # Query3
 * # This query is similar to Query 1 but class Publication has a wide hierarchy.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X
 * WHERE
 * {?X rdf:type ub:Publication .
 * ?X ub:publicationAuthor
 * http://www.Department0.University0.edu/AssistantProfessor0}
 */
object JenaQuery3 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 3")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X", rdf + "type", ub + "Publication"),
      Array("X", ub + "publicationAuthor", "http://www.Department0.University0.edu/AssistantProfessor0")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X")).withName("Project variable X")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
