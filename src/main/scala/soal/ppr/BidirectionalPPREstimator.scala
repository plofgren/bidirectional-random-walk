package soal.ppr

import java.util.Random

import com.twitter.cassovary.graph.{DirectedGraph, Node}
import soal.util._

import scala.collection.mutable

/**
 * Contains methods related to personalized PageRank estimation.  All methods operate in the
 * context of the given graph, teleportProbability, and random.  See the paper
 * "Personalized PageRank Estimation and Search: A Bidirectional Approach" by Lofgren, Banerjee,
 * and Goel for more information on the algorithm implemented.
 */

class BidirectionalPPREstimator (val graph: DirectedGraph[Node],
                                 val teleportProbability: Float,
                                 val random: Random = new Random) {
  /**
   * Estimates the personalized PageRank score from the given source to the given target.  The the
   * true score is greater than the given minimumPPR, then the result will have mean relative
   * error less than the given bound.  If guaranteeRlativeError is set, then the relative error
   * bound will be guaranteed (at the cost of slower execution); otherwise the relative error is
   * only smaller than the given relativeError bound on average (based on experiments in the
   * above publication).
   */
  def estimatePPR(sourceDistribution: DiscreteDistribution,
                  targetId: Int,
                  minimumPPR: Float = 1.0f / graph.nodeCount,
                  relativeError: Float = 0.1f,
                  guaranteeRelativeError: Boolean = false): Float = {
    val chernoffConstant = if (guaranteeRelativeError)
      3 * math.log(2 / 1.0e-9) // guarantees the given relative error with probability 1 - 1.0e-9
    else
      0.07 // Found in experiments to give mean relative error less than 10% when relativeError=10%

    val chernoffMultiplier = chernoffConstant / math.pow(relativeError, 2.0).toFloat

    def computeWalkCount(maxResidual: Float): Int =
      (chernoffMultiplier * maxResidual / minimumPPR).toInt
    val msPerWalk = estimateMsPerWalk(sourceDistribution)
    def estimateForwardTimeInMillis(maxResidual: Float): Float =
      computeWalkCount(maxResidual) * msPerWalk
    val (estimates, residuals, maxResidual) =
      computeContributionsBalanced(targetId, estimateForwardTimeInMillis)

    val walkCount = computeWalkCount(maxResidual)
    var estimate = sourceDistribution.expectation(estimates)
    for (walkIndex <- 0 until walkCount) {
      val v = samplePPR(sourceDistribution)
      estimate += residuals(v) / walkCount
    }

    estimate
  }

  /**
   * Computes personalized PageRank from a source node to a target node. (Convenience method that
   * calls the above method)
   */
  def estimatePPRSingleSource(sourceId: Int,
                              targetId: Int,
                              minimumPPR: Float = 1.0f / graph.nodeCount,
                              relativeError: Float = 0.1f,
                              guaranteeRelativeError: Boolean = false): Float =
    estimatePPR(new ConstantDistribution(sourceId), targetId,
      minimumPPR, relativeError, guaranteeRelativeError)

  /** Given a target nodeId, returns a map estimates such that estimates(v)
    * approximates ppr(v, target) with additive error pprErrorTolerance.
    */
  def estimatePPRToTarget(targetId: Int, pprErrorTolerance: Float): mutable.Map[Int, Float] =
    computeContributions(targetId, pprErrorTolerance)._1

  /** Given a target nodeId, returns two maps (estimates, residuals). The value estimates(v)
    * approximates ppr(v, target) with additive error maxResidual, and the residuals satisfy a
    * useful loop invariant.  See the paper "Local Computation of PageRank Contributions" for details.
    */
  private def computeContributions(targetId: Int, maxResidual: Float,
                           edgeWeight: (Node, Node) => Float = (u: Node, v: Node) => 1.0f / u.outboundCount
      ): (mutable.Map[Int, Float], mutable.Map[Int, Float]) = {
    // Store all nodes with residual greater than maxResidual in a queue to be processed
    // Use ArrayDequeue because it is more efficient than the linked-list based mutable.Queue
    val largeResidualNodes = new java.util.ArrayDeque[Int]()
    largeResidualNodes.add(targetId)

    // estimates(uId) estimates ppr(u, target)
    val estimates = CollectionsUtil.efficientIntFloatMap().withDefaultValue(0.0f)
    val residuals = CollectionsUtil.efficientIntFloatMap().withDefaultValue(0.0f)
    residuals(targetId) =  1.0f

    while (!largeResidualNodes.isEmpty) {
      val vId = largeResidualNodes.pollFirst()
      val vResidual = residuals(vId)
      estimates(vId) += teleportProbability * residuals(vId)
      residuals(vId) = 0.0f
      val v = graph.getNodeById(vId).get
      for (uId <- v.inboundNodes()) {
        val u = graph.getNodeById(uId).get
        val residualChange = (1.0f - teleportProbability) * edgeWeight(u, v) * vResidual
        if (residuals(uId) < maxResidual && residuals(uId) + residualChange >= maxResidual)
          largeResidualNodes.add(uId)
        residuals(uId) += residualChange
      }
    }
    (estimates, residuals)
  }

  /**
   * Variant of computeContributions used in balanced Bidirectional-PPR.  Does reverse pushes,
   * decreasing maxResidual incrementally, until the time spent equals the remaining time required
   * for forward walks, as estimated by the given function.
   *
   * Returns estimates, residuals, and maximum residual.
   */
  private def computeContributionsBalanced(targetId: Int,
                                             forwardMillisGivenMaxResidual: Float => Double
      ): (collection.Map[Int, Float], collection.Map[Int, Float], Float) = {
    val priorityQueue = new HeapMappedIntPriorityQueue()
    priorityQueue.insert(targetId, 1.0f)

    // estimates(uId) estimates ppr(u, target)
    val estimates = CollectionsUtil.efficientIntFloatMapWithDefault0()
    val startTime = System.currentTimeMillis()
    def elapsedTime(): Double = System.currentTimeMillis() - startTime
    while (!priorityQueue.isEmpty &&
      elapsedTime() < forwardMillisGivenMaxResidual(priorityQueue.maxPriority)) {
      val vResidual = priorityQueue.maxPriority
      val vId = priorityQueue.extractMax()
      estimates(vId) += teleportProbability * vResidual
      val v = graph.getNodeById(vId).get
      for (uId <- v.inboundNodes()) {
        val u = graph.getNodeById(uId).get
        val residualChange = (1.0f - teleportProbability) * vResidual / u.outboundCount
        priorityQueue.increasePriority(uId, priorityQueue.getPriority(uId) + residualChange)
      }
    }
    val maxResidual = if (priorityQueue.isEmpty) 0.0f else priorityQueue.maxPriority
    val residuals = priorityQueue.currentPriorities()
    (estimates, residuals, maxResidual)
  }

  /**
   * Samples from the PageRank distribution personalized to the given source by doing a single
   * random walk.
   */
  def samplePPR(sourceId: Int): Int =
    samplePPR(new ConstantDistribution(sourceId))

  /**
   * Samples from the PageRank distribution personalized to the given source distribution by
   * doing a single random walk.
   */
  def samplePPR(sourceDistribution: DiscreteDistribution): Int = {
    var v = sourceDistribution.sample()
    while(random.nextFloat() > teleportProbability) {
      val vNode = graph.getNodeById(v).get
      vNode.randomOutboundNode(random) match {
        case Some(outNeighbor) =>
          v = outNeighbor
        case None =>
          v = sourceDistribution.sample() // Teleport after reaching dead end.
      }
    }
    v
  }

  /** Does the given number of walks on the given graph and returns the mean time per walk, in
    * milliseconds.  Used to autmatically balance forward and reverse work. */
  def estimateMsPerWalk(sourceDistribution: DiscreteDistribution, walkCount: Int = 1000): Float = {
    val millisPerNano = 1.0e6f
    val startTime = System.nanoTime()
    for (i <- 0 until walkCount) {
      samplePPR(sourceDistribution)
    }
    (System.nanoTime() - startTime).toFloat / walkCount * millisPerNano
  }
}
