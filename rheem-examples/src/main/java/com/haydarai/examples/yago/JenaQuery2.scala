package com.haydarai.examples.yago

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

object JenaQuery2 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("YAGO3: Query 2")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val y = "http://yago-knowledge.org/resource/"

    // Define triples definition
    val triples = List[Array[String]](
      Array("p", y + "wasBornIn", "c"),
      Array("p", y + "hasAcademicAdvisor", "a"),
      Array("a", y + "wasBornIn", "c"),
      Array("p", y + "isMarriedTo", "p2"),
      Array("p2", y + "wasBornIn", "c")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("p", "p2", "a", "c")).withName("Project variable ?p ?p2 ?a ?c")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
