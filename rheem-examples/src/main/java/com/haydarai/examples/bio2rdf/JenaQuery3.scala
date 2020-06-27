package com.haydarai.examples.bio2rdf

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

import scala.collection.JavaConverters._

object JenaQuery3 {
  def main(args: Array[String]) {
    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("Bio2RDF: Query 3")
      .withUdfJarsOf(this.getClass)

    // Prefix definition
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val rdfs = "http://www.w3.org/2000/01/rdf-schema#"
    val omim = "http://bio2rdf.org/omim_vocabulary:"

    // Define triples definition
    val triples = List[Array[String]](
      Array("phenotype", rdf + "type", omim + "Phenotype"),
      Array("phenotype", rdfs + "label", "label"),
      Array("gene", omim + "phenotype", "phenotype")
    )

    val records = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava)).withName("Read RDF file")
      .projectRecords(List("phenotype")).withName("Project variable phenotype")
      .collect()

    // Print query result
    records.foreach(println)
  }
}
