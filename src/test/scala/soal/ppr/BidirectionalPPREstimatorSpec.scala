package soal.ppr

import java.util.Random

import co.teapot.graph.ConcurrentHashMapDynamicGraph
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.io.Source

class BidirectionalPPREstimatorSpec extends FlatSpec with Matchers {
  val graph = ConcurrentHashMapDynamicGraph.readGraph("src/test/resources/test_graph.txt")
  val teleportProb = 0.2f
  val random = new Random(2) // Seed for consistent tests
  val estimator = new BidirectionalPPREstimator(graph, teleportProb, random)
  val truePPRs = BidirectionalPPREstimatorSpec.testGraphTruePPRs

  "BidirectionalPPRSearcher.estimateInversePPR" should "be correct on the test graph" in {
    val pprErrorTolerance = 2.0e-6f
    for (((s, t), truePPR) <- truePPRs) {
      val inversePPRs = estimator.estimatePPRToTarget(t, pprErrorTolerance)
      withClue (s"Testing Pair ($s, $t)") {
        inversePPRs(s) should equal (truePPR +- pprErrorTolerance)
      }
    }
  }

  "BidirectionalPPRSearcher.estimatePPR" should "be correct on the test graph" in {
    val relativeError = 0.01f
    val stPairs = Array(0 -> 1, 2 -> 3, 5 -> 9, 0 -> 0)

    for ((s, t) <- stPairs) {
      withClue (s"Testing Pair ($s, $t)") {
        estimator.estimatePPRSingleSource(s, t, 0.03f, relativeError) should equal (
          truePPRs((s, t)) +- truePPRs((s, t)) * relativeError * 2)
      }
    }
  }
}

object BidirectionalPPREstimatorSpec {
  def testGraphTruePPRs: collection.Map[(Int, Int), Float] = {
    val pprMap = new mutable.HashMap[(Int, Int), Float] {
      override def default(key: (Int, Int)) = 0.0f
    }
    for (line <- Source.fromFile("src/test/resources/test_graph_true_pprs.txt").getLines()) {
      val pieces = line.split("\t")
      val (startId, targetId, truePPR) = (pieces(0).toInt, pieces(1).toInt, pieces(2).toFloat)
      pprMap((startId, targetId)) = truePPR
    }
    pprMap
  }
}
