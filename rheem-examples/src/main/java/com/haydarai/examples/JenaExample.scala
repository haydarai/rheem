package com.haydarai.examples

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource
import scala.collection.JavaConverters._

object JenaExample {
  def main(args: Array[String]) {

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("Jena")
      .withUdfJarsOf(this.getClass)

    val triples = List[Array[String]](
      Array("p", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf", "c"),
      Array("s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse", "c"),
      Array("s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#advisor", "p")
    )

    var results = planBuilder.readModel(new JenaModelSource(args(0), triples.asJava))
    results = results.projectRecords(List("s"))
    val res = results.collect();
    res.foreach(println)
  }
}
