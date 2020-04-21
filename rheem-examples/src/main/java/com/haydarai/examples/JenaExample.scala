package com.haydarai.examples

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource

object JenaExample {
  def main(args: Array[String]) {

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("Jena")
      .withUdfJarsOf(this.getClass)

//    var results = planBuilder.readModel(new JenaModelSource(args(0), "s", "p", "o"))
//    results = results.projectRecords(List("s"))
//    val res = results.collect();
//    res.foreach(println)
  }
}
