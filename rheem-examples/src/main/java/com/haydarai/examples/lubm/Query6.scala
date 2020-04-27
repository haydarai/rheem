package com.haydarai.examples.lubm

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

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
object Query6 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 6")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X", rdf + "type", ub + "UndergraduateStudent")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X")).withName("Project variable ?X")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
