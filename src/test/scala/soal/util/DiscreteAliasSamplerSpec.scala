package soal.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import java.util.Random

/*
 * Created by plofgren on 4/16/15.
 */

class DiscreteAliasSamplerSpec  extends FlatSpec with Matchers  {
  val random = new Random(1)
  def testDistribution(unnormalizedProbabilities: Array[Float],
                       values: Seq[Int],
                       nSamples: Int = 10000
      ): Unit = {
    val probabilities = unnormalizedProbabilities map { _ / unnormalizedProbabilities.sum }
    val n = unnormalizedProbabilities.size
    val valueToIndex = (values zip (0 until n)).toMap
    val sampler = new DiscreteAliasSampler(values, unnormalizedProbabilities, random)
    val sampleCounts = Array.fill(n)(0)
    val tol = 4.0f / math.sqrt(nSamples).toFloat
    for (i <- 0 until nSamples) {
      val v = sampler.sample()
      sampleCounts(valueToIndex(v)) += 1
    }
    for (i <- 0 until n) {
      sampleCounts(i).toFloat / nSamples should equal (probabilities(i) +- tol)
    }

    def f(v: Int): Float = v.toFloat * v.toFloat // compute expectation of v => v^2
    val trueExpectation = ((probabilities zip values) map { case (p, v) => p * v * v }).sum
    sampler.expectation(f) shouldEqual (trueExpectation +- trueExpectation * 1.00001f)
  }

  "A Discrete Distribution" should "support sampling" in {
    testDistribution(Array(575.6355f, 89.733475f, 86.90718f, 721.26416f), Array(2, 3, 5, 7))
    testDistribution(Array(2.0f, 5.0f, 3.0f), Array(17, 11, 13))
    testDistribution(Array(1.0f, 1.0f, 1.0f, 1.0f), Array(-2, 3, -5, 7))
    testDistribution(Array(0.9f, 0.1f), Array(19, 17))
    an[IllegalArgumentException] should be thrownBy {
      new DiscreteAliasSampler(Array(1), Array(1.0f, 2.0f))
    }
  }
}
