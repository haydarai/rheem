package com.haydarai.examples.lubm

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.basic.data.Tuple3
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

/**
 * # Query1
 * # This query bears large input and high selectivity. It queries about just one class and
 * # one property and does not assume any hierarchy information or inference.
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?X
 * WHERE
 * {?X rdf:type ub:GraduateStudent .
 * ?X ub:takesCourse
 * http://www.Department0.University0.edu/GraduateCourse0}
 */
object SparkQuery1 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("LUBM: Query 1")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val ub = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"

    val triples = planBuilder
      .readTextFile("file:" + args(0))
      .map(parseTriple)

    val projectTriple1 = triples
      .filter(_.field1.equals(rdf + "type"))
      .filter(_.field2.equals(ub + "GraduateStudent"))
      .map(record => record.field0)

    val projectTriple2 = triples
      .filter(_.field1.equals(ub + "takesCourse"))
      .filter(_.field2.equals("http://www.Department0.University0.edu/GraduateCourse0"))
      .map(record => record.field0)

    val records = projectTriple1
      .join[String, String](_.toString, projectTriple2, _.toString)
      .map(tuple => tuple.field0)
      .collect()

    // Print query result
    records.foreach(println)
  }

  def parseTriple(raw: String): Tuple3[String, String, String] = {
    // Find the first two spaces: Odds are that these are separate subject, predicated and object.
    val firstSpacePos = raw.indexOf(' ')
    val secondSpacePos = raw.indexOf(' ', firstSpacePos + 1)

    // Find the end position.
    var stopPos = raw.lastIndexOf('.')
    while (raw.charAt(stopPos - 1) == ' ') stopPos -= 1

    try {
      val s = raw.substring(1, firstSpacePos - 1)
      val p = raw.substring(firstSpacePos + 2, secondSpacePos - 1)
      val o = raw.substring(secondSpacePos + 2, stopPos - 1)

      new Tuple3(s, p, o)
    } catch {
      case _: StringIndexOutOfBoundsException => new Tuple3("", "", "")
    }
  }
}
