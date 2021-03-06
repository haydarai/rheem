package org.qcri.rheem.api.graph

import org.qcri.rheem.api._
import org.qcri.rheem.basic.data.Record
import org.qcri.rheem.basic.operators.{DegreeCentralityOperator, MapOperator, PageRankOperator, SingleSourceShortestPathOperator}
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval

/**
  * This class enhances the functionality of [[DataQuanta]] with [[Record]]s.
  */
class EdgeDataQuanta(dataQuanta: DataQuanta[Edge]) {

  implicit def planBuilder: PlanBuilder = dataQuanta.planBuilder

  /**
    * Feed this instance into a [[PageRankOperator]].
    *
    * @param numIterations number of PageRank iterations
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def pageRank(numIterations: Int = 20,
               dampingFactor: Double = PageRankOperator.DEFAULT_DAMPING_FACTOR,
               graphDensity: ProbabilisticDoubleInterval = PageRankOperator.DEFAULT_GRAPH_DENSITIY):
  DataQuanta[PageRank] = {
    val pageRankOperator = new PageRankOperator(numIterations, dampingFactor, graphDensity)
    dataQuanta.connectTo(pageRankOperator, 0)
    wrap[PageRank](pageRankOperator)
  }

  def degreeCentrality():
  DataQuanta[DegreeCentrality] = {
    val degreeCentralityOperator = new DegreeCentralityOperator
    dataQuanta.connectTo(degreeCentralityOperator, 0)
    wrap[DegreeCentrality](degreeCentralityOperator)
  }

  def singleSourceShortestPath(sourceId: Long):
  DataQuanta[SingleSourceShortestPath] = {
    val singleSourceShortestPathOperator = new SingleSourceShortestPathOperator(sourceId)
    dataQuanta.connectTo(singleSourceShortestPathOperator, 0)
    wrap[SingleSourceShortestPath](singleSourceShortestPathOperator)
  }
}