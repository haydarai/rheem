package org.qcri.rheem.spark.operators.graph

import java.lang.{Long => JavaLong}
import java.util
import java.util.Collections

import org.apache.spark.graphx.{Graph, VertexId}
import org.qcri.rheem.basic.data.{Tuple2 => T2}
import org.qcri.rheem.basic.operators.{PageRankOperator, SingleSourceShortestPathOperator}
import org.qcri.rheem.core.optimizer.OptimizationContext
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode
import org.qcri.rheem.core.platform.{ChannelDescriptor, ChannelInstance}
import org.qcri.rheem.spark.channels.RddChannel
import org.qcri.rheem.spark.execution.SparkExecutor
import org.qcri.rheem.spark.operators.SparkExecutionOperator

/**
 * GraphX-based implementation of the [[PageRankOperator]].
 */
class SparkSingleSourceShortestPathOperator(_sourceId: Long)
  extends SingleSourceShortestPathOperator(_sourceId) with SparkExecutionOperator {

  def this(that: SingleSourceShortestPathOperator) = this(that.getSourceId)

  override def evaluate(inputs: Array[ChannelInstance],
                        outputs: Array[ChannelInstance],
                        sparkExecutor: SparkExecutor,
                        operatorContext: OptimizationContext#OperatorContext) = {
    val input = inputs(0).asInstanceOf[RddChannel#Instance]
    val output = outputs(0).asInstanceOf[RddChannel#Instance]

    val edgeRdd = input.provideRdd[T2[JavaLong, JavaLong]]().rdd
      .map(edge => (edge.field0.longValue, edge.field1.longValue))

    input.provideRdd[T2[JavaLong, JavaLong]]().rdd.map(edge => println(edge))
    var graph = Graph.fromEdgeTuples(edgeRdd, 0.0)
    val sourceId: VertexId = _sourceId

    graph = graph.mapVertices((id, _) => {
      if (id == sourceId) 0.0
      else Double.PositiveInfinity
    })

    val sssp = graph.pregel(Double.PositiveInfinity)(
      (_, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    val resultRdd = sssp.vertices
      .map { case (vertexId, distance) => new T2(vertexId, distance.toFloat) }
      .toJavaRDD

    output.accept(resultRdd, sparkExecutor)

    val mainExecutionLineageNode = new ExecutionLineageNode(operatorContext)
    mainExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
      "rheem.spark.pagerank.load.main", sparkExecutor.getConfiguration
    ))
    mainExecutionLineageNode.addPredecessor(input.getLineage)

    val outputExecutionLineageNode = new ExecutionLineageNode(operatorContext)
    outputExecutionLineageNode.add(LoadProfileEstimators.createFromSpecification(
      "rheem.spark.pagerank.load.output", sparkExecutor.getConfiguration
    ))
    output.getLineage.addPredecessor(outputExecutionLineageNode)

    mainExecutionLineageNode.collectAndMark()
  }

  override def getLoadProfileEstimatorConfigurationKeys: java.util.Collection[String] =
    java.util.Arrays.asList("rheem.spark.pagerank.load.main", "rheem.spark.pagerank.load.output")

  override def getSupportedInputChannels(index: Int): util.List[ChannelDescriptor] = {
    assert(index == 0)
    Collections.singletonList(RddChannel.CACHED_DESCRIPTOR)
  }

  override def getSupportedOutputChannels(index: Int): util.List[ChannelDescriptor] = {
    assert(index == 0)
    Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR)
  }

  override def containsAction(): Boolean = true
}
