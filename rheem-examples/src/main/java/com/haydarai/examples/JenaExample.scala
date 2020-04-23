package com.haydarai.examples

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.api.graph._
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.basic.data.{Tuple2, Tuple3}
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.java.Java
import org.qcri.rheem.jena.Jena
import org.qcri.rheem.jena.operators.JenaModelSource
import org.qcri.rheem.spark.Spark

import scala.collection.JavaConverters._

object JenaExample {
  def main(args: Array[String]) {

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(RheemBasics.graphPlugin)
      .withPlugin(Jena.plugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Spark.graphPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("Jena")
      .withUdfJarsOf(this.getClass)

    val triples = List[Array[String]](
      Array("p", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf", "c"),
      Array("s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse", "c"),
      Array("s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#advisor", "p")
    )

    val projectedRecords = planBuilder
      .readModel(new JenaModelSource(args(0), triples.asJava))
      .projectRecords(List("s", "p", "c"))

    val edgesPageRank = projectedRecords
      .map(record => (record.getField(0).toString, record.getField(1).toString))

    val edgesCentrality = projectedRecords
      .flatMap(record => Seq((record.getField(0).toString, record.getField(2).toString), (record.getField(1).toString, record.getField(2).toString)))

    val vertexIds = projectedRecords
      .flatMap(record => Seq(record.getField(0).toString, record.getField(1).toString, record.getField(2).toString))
      .distinct
      .zipWithId

    type VertexId = Tuple2[Vertex, String]
    val idEdgesPageRank = edgesPageRank
      .join[VertexId, String](_._1, vertexIds, _.field1)
      .map { linkAndVertexId =>
        (linkAndVertexId.field1.field0, linkAndVertexId.field0._2)
      }
      .join[VertexId, String](_._2, vertexIds, _.field1)
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))
    val idEdgesDegreeCentrality = edgesCentrality
      .join[VertexId, String](_._1, vertexIds, _.field1)
      .map { linkAndVertexId =>
        (linkAndVertexId.field1.field0, linkAndVertexId.field0._2)
      }
      .join[VertexId, String](_._2, vertexIds, _.field1)
      .map(linkAndVertexId => new Edge(linkAndVertexId.field0._1, linkAndVertexId.field1.field0))

    val pageRanks = idEdgesPageRank.pageRank(10)
    val degreeCentrality = idEdgesDegreeCentrality.degreeCentrality()

    val pageRankResults = pageRanks
      .join[VertexId, Long](_.field0, vertexIds, _.field0)
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1.toString))

    val degreeCentralityResults = degreeCentrality
      .join[VertexId, Long](_.field0, vertexIds, _.field0)
      .map(joinTuple => (joinTuple.field1.field1, joinTuple.field0.field1.toString))

    val records = projectedRecords
      .map(record => new Tuple3(record.getField(0).toString, record.getField(1).toString, record.getField(2).toString))
      .join[(String, String), String](_.field1, pageRankResults, _._1)
      .map(tuple2 => new Tuple3(tuple2.field0.field0, tuple2.field1._2, tuple2.field0.field2))
      .join[(String, String), String](_.field2, degreeCentralityResults, _._1)
      .map(tuple2 => new Tuple3(tuple2.field0.field0, tuple2.field0.field1, tuple2.field1._2))
      .collect()

    records.foreach(println)
  }
}
