package org.qcri.rheem.spark.operators.graph

import java.lang.{Long => JavaLong}
import java.util
import java.util.Collections

import org.apache.spark.graphx.{Graph, VertexId}
import org.qcri.rheem.basic.data.{Tuple2 => T2}
import org.qcri.rheem.basic.operators.DegreeCentralityOperator
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators
import org.qcri.rheem.core.optimizer.{OptimizationContext, ProbabilisticDoubleInterval}
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode
import org.qcri.rheem.core.platform.{ChannelDescriptor, ChannelInstance}
import org.qcri.rheem.core.util.Tuple
import org.qcri.rheem.spark.channels.RddChannel
import org.qcri.rheem.spark.execution.SparkExecutor
import org.qcri.rheem.spark.operators.SparkExecutionOperator

class SparkDegreeCentralityOperator extends DegreeCentralityOperator() with SparkExecutionOperator {
  def this(that: DegreeCentralityOperator) = this()

  /**
   * Evaluates this operator. Takes a set of {@link ChannelInstance}s according to the operator inputs and manipulates
   * a set of {@link ChannelInstance}s according to the operator outputs -- unless the operator is a sink, then it triggers
   * execution.
   * <p>In addition, this method should give feedback of what this instance was doing by wiring the
   * {@link org.qcri.rheem.core.platform.lineage.LazyExecutionLineageNode}s of input and ouput {@link ChannelInstance}s and
   * providing a {@link Collection} of executed {@link ExecutionLineageNode}s.</p>
   *
   * @param inputs { @link ChannelInstance}s that satisfy the inputs of this operator
   * @param outputs { @link ChannelInstance}s that accept the outputs of this operator
   * @param sparkExecutor { @link SparkExecutor} that executes this instance
   * @param operatorContext optimization information for this instance
   * @return { @link Collection}s of what has been executed and produced
   */
  override def evaluate(inputs: Array[ChannelInstance],
                        outputs: Array[ChannelInstance],
                        sparkExecutor: SparkExecutor,
                        operatorContext: OptimizationContext#OperatorContext): Tuple[util.Collection[ExecutionLineageNode], util.Collection[ChannelInstance]] = {
    val input = inputs(0).asInstanceOf[RddChannel#Instance]
    val output = outputs(0).asInstanceOf[RddChannel#Instance]

    val edgeRdd = input.provideRdd[T2[JavaLong, JavaLong]]().rdd
      .map(edge => (edge.field0.longValue, edge.field1.longValue))
    val graph = Graph.fromEdgeTuples(edgeRdd, null)

    val resultRdd = graph.degrees
      .map { case (vertexId: VertexId, degree) => new T2(vertexId, degree.toInt) }
      .toJavaRDD()

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

  /**
   * Tell whether this instances is a Spark action. This is important to keep track on when Spark is actually
   * initialized.
   *
   * @return whether this instance issues Spark actions
   */
  override def containsAction(): Boolean = true

  /**
   * Display the supported {@link Channel}s for a certain {@link InputSlot}.
   *
   * @param index the index of the { @link InputSlot}
   * @return an { @link List} of { @link Channel}s' { @link Class}es, ordered by their preference of use
   */
  override def getSupportedInputChannels(index: Int): util.List[ChannelDescriptor] = {
    assert(index == 0)
    Collections.singletonList(RddChannel.CACHED_DESCRIPTOR)
  }

  /**
   * Display the supported {@link Channel}s for a certain {@link OutputSlot}.
   *
   * @param index the index of the { @link OutputSlot}
   * @return an { @link List} of { @link Channel}s' { @link Class}es, ordered by their preference of use
   * @see #getOutputChannelDescriptor(int)
   * @deprecated { @link ExecutionOperator}s should only support a single { @link ChannelDescriptor}
   */
  override def getSupportedOutputChannels(index: Int): util.List[ChannelDescriptor] = {
    assert(index == 0)
    Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR)
  }
}
