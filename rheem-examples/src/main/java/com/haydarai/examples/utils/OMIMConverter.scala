package com.haydarai.examples.utils

import java.io.File

import org.qcri.rheem.api.PlanBuilder
import org.qcri.rheem.basic.RheemBasics
import org.qcri.rheem.basic.data.Tuple3
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.util.fs.LocalFileSystem
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

object OMIMConverter {
  def main(args: Array[String]) {

    // Get a plan builder.
    val rheemContext = new RheemContext(new Configuration)
      .withPlugin(RheemBasics.graphPlugin)
      .withPlugin(Java.basicPlugin)
      .withPlugin(Java.channelConversionPlugin)
      .withPlugin(Spark.basicPlugin)
      .withPlugin(Spark.graphPlugin)

    val planBuilder = new PlanBuilder(rheemContext)
      .withJobName("OMIM converter")
      .withUdfJarsOf(this.getClass)

    val triples = planBuilder
      .readTextFile("file:" + args(0))
      .map(parseTriple)

    val targetUrl = LocalFileSystem.toURL(new File(args(1)))
    triples.writeTextFile(targetUrl, t => {
      val triple = t.field0 + " " + t.field1 + " " + t.field2.replaceAll("\"\"\"\"", "\"").replaceAll("\\|\\|", "")
      if (triple.startsWith("<") && !t.field1.equalsIgnoreCase("<http://purl.org/dc/terms/description>")) {
        triple + " ."
      } else {
        ""
      }
    })
  }

  def parseTriple(raw: String): Tuple3[String, String, String] = {
    try {
      // Find the first two spaces: Odds are that these are separate subject, predicated and object.
      val firstSpacePos = raw.indexOf(' ')
      val secondSpacePos = raw.indexOf(' ', firstSpacePos + 1)

      // Find the end position.
      var stopPos = raw.lastIndexOf('.')
      while (raw.charAt(stopPos - 1) == ' ') stopPos -= 1

      val s = raw.substring(0, firstSpacePos)
      val p = raw.substring(firstSpacePos + 1, secondSpacePos)
      val o = raw.substring(secondSpacePos + 1, stopPos)

      new Tuple3(s, p, o)
    } catch {
      case _: StringIndexOutOfBoundsException => new Tuple3("", "", "")
    }
  }
}
