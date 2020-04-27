package com.haydarai.examples.database

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.postgres.Postgres
import org.qcri.rheem.postgres.operators.PostgresTableSource

object PostgresExample {
  def main(args: Array[String]) {

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(Postgres.plugin)
      .withPlugin(Java.basicPlugin)

    val jdbcUrl = "jdbc:postgresql://localhost:5432/dbpedia"
    rheemContext.getConfiguration.setProperty("rheem.postgres.jdbc.url", jdbcUrl)
    rheemContext.getConfiguration.setProperty("rheem.postgres.jdbc.user", "postgres")
    rheemContext.getConfiguration.setProperty("rheem.postgres.jdbc.password", "postgres")

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("SQLite")
      .withUdfJarsOf(this.getClass)

    val records = planBuilder.readTable(new PostgresTableSource("dbpedia", "subject", "predicate", "object"))
    val results = records.projectRecords(List("object")).collect();

    results.foreach(println)
  }
}
