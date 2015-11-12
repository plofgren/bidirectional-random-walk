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
                        nSamples: Int = 10000,
                        valueOffset: Int = 7 // valueOffset tests index <-> value mapping
                        ): Unit = {
    val probabilities = unnormalizedProbabilities map { _ / unnormalizedProbabilities.sum }
    val n = unnormalizedProbabilities.size
    val values = ((0 until n) map { _ + valueOffset }).toArray
    val sampler = new DiscreteAliasSampler(values, unnormalizedProbabilities, random)
    val sampleCounts = Array.fill(n)(0)
    val tol = 4.0f / math.sqrt(nSamples).toFloat
    for (i <- 0 until nSamples) {
      sampleCounts(sampler.sample() - valueOffset) += 1
    }
    for (i <- 0 until n) {
      sampleCounts(i).toFloat / nSamples should equal (probabilities(i) +- tol)
    }
  }
  "A Discrete Distribution" should "support sampling" in {
    testDistribution(Array(575.6355f, 89.733475f, 86.90718f, 721.26416f))
    testDistribution(Array(2.0f, 5.0f, 3.0f))
    testDistribution(Array(1.0f, 1.0f, 1.0f, 1.0f))
    testDistribution(Array(0.9f, 0.1f))
    an[IllegalArgumentException] should be thrownBy {
      new DiscreteAliasSampler(Array(1), Array(1.0f, 2.0f))
    }
  }
}
