package soal.util

import java.util.Random
import scala.collection._
import java.util

/**
 *  Represents a discrete distribution which supports sampling value values(i) with probability
 *  proportional to unnormalizedProbabilities(i).  Construction is O(n) time and sampling is O(1)
  *  time. Uses the alias method (see links at http://en.wikipedia.org/wiki/Alias_method).
 */
class DiscreteAliasSampler(values: Seq[Int],
                           unnormalizedProbabilities: Seq[Float],
                           val random: Random = new Random()
    ) extends DiscreteDistribution {
  require(values.size == unnormalizedProbabilities.size)
  val n = values.size
  val normalizingConstant = (unnormalizedProbabilities map { _.toDouble }).sum
  val normalizedProbabilities = unnormalizedProbabilities.toArray map {
    _.toDouble / normalizingConstant
  }
  // There are n buckets, each with a "left" part and a "right" part
  // To sample, we sample a uniform random bucket index i, then return the left value with
  // probability leftProbability(i)
  val leftValues = Array.fill(n)(-123456)//new Array[Int](n)
  val rightValues = Array.fill(n)(-123456)
  val leftProbabilities = new Array[Double](n)

  private def constructBuckets(): Unit = {
    val average = 1.0 / n

    val remainingProbabilities = normalizedProbabilities.clone()
    val smallIndices = new util.Stack[Int]
    val largeIndices = new util.Stack[Int]
    def pushIndex(i: Int) {
      (if (remainingProbabilities(i) <= average * (1 + 1.0e-9)) smallIndices else largeIndices)
        .push(i)
    }
    (0 until n) foreach pushIndex

    for (bucketI <- 0 until n) {
      assert(! smallIndices.empty(),  "Alias method failed for probabilities " +
        unnormalizedProbabilities)
      val smallI = smallIndices.pop()
      leftValues(bucketI) = values(smallI)
      leftProbabilities(bucketI) = remainingProbabilities(smallI) * n
      remainingProbabilities(smallI) = 0.0
//      if (!smallIndices.empty) {
//
//      } else {
//        leftProbabilities(bucketI) = 0.0
//      }
      if (!largeIndices.empty) {
        val largeI = largeIndices.pop()
        rightValues(bucketI) = values(largeI)
        remainingProbabilities(largeI) -= (1.0 - leftProbabilities(bucketI)) / n
        pushIndex(largeI)
      }
    }
    assert(remainingProbabilities forall { _ < 1.0e-8}, "Alias method failed for probabilities "
      + unnormalizedProbabilities + "\nRemaining probabilities " + remainingProbabilities.toSeq)
  }
  constructBuckets()

  def sample(): Int = {
    val i = random.nextInt(n)
    val chooseLeft = random.nextFloat() < leftProbabilities(i)
    if (chooseLeft)
      leftValues(i)
    else
      rightValues(i)
  }

  //def expectation(f: Int => Float): Float = {
  //  ((0 until n) map { i =>
  //    leftProbabilities(i) * f(leftValues(i)) + (1.0f - leftProbabilities(i)) * f(rightValues(i))
  //  }).sum.toFloat / n
  //}

  def size: Int = n

  override def valuesAndProbabilities(): Iterable[(Int, Float)] =
    values zip (normalizedProbabilities map {_.toFloat})
}

object DiscreteAliasSampler {
  def apply(valueWeights: Map[Int, Float],
           random: Random = new Random()): DiscreteAliasSampler = {
    val (values, probabilities) = valueWeights.toSeq.unzip
    new DiscreteAliasSampler(values, probabilities, random)
  }
}
