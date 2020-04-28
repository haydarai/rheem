package com.haydarai.examples.lubm

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

/**
 * # Query13
 * # Property hasAlumnus is defined in the benchmark ontology as the inverse of
 * # property degreeFrom, which has three subproperties: undergraduateDegreeFrom,
 * # mastersDegreeFrom, and doctoralDegreeFrom. The benchmark data state a person as
 * # an alumnus of a university using one of these three subproperties instead of
 * # hasAlumnus. Therefore, this query assumes subPropertyOf relationships between
 * # degreeFrom and its subproperties, and also requires inference about inverseOf.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X
 * WHERE
 * {?X rdf:type ub:Person .
 * <http://www.University0.edu> ub:hasAlumnus ?X}
 */
object Query13 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 13")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    // Define triples definition
    val triples = List[Array[String]](
      Array("X", rdf + "type", ub + "GraduateStudent")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("X")).withName("Project variable ?X")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
