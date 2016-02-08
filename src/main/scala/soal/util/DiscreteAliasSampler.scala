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
  // There are n buckets, each with a "left" value and a "right" value
  // To sample, we sample a uniform random bucket index i, then return the left value with
  // probability leftProbability(i), and the right value with the remaining probability.
  val leftValues = new Array[Int](n)
  val rightValues = new Array[Int](n)
  val leftProbabilities = new Array[Float](n)

  constructBuckets()

  override def sample(): Int = {
    val i = random.nextInt(n)
    val chooseLeft = random.nextFloat() < leftProbabilities(i)
    if (chooseLeft)
      leftValues(i)
    else
      rightValues(i)
  }

  /* Because we don't want to store normalized probabilities, compute the expectation by summing
  over the left and right value in each bucket.
   */
  override def expectation(f: Int => Float): Float = {
    ((0 until n) map { i =>
      leftProbabilities(i) * f(leftValues(i)) + (1.0f - leftProbabilities(i)) * f(rightValues(i))
    }).sum / n
  }

  def size: Int = n

  private def constructBuckets(): Unit = {
    val average = 1.0 / n
    /* Strategy:
      Each index has a "remaining probability" which starts equal to its (normalized) probability
      and decreases as the index is placed in buckets.
      Define an index to be "large" if its current remaining probability is greater than 1.0/n, and
      "small" otherwise.
      We maintain stacks of small indices and large indices.  Repeatedly we will pop off a
      small index, place it in the left part of a bucket, and fill the right part of the bucket
      with a large index, updating the remaining probabilities accordingly.
      */
    val normalizingConstant = unnormalizedProbabilities.sum
    val remainingProbabilities = unnormalizedProbabilities.toArray map { _ / normalizingConstant }
    val smallIndices = new util.Stack[Int]
    val largeIndices = new util.Stack[Int]

    def isSmall(i: Int): Boolean = remainingProbabilities(i) <= average * (1 + 1.0e-5f)
    def pushIndexInCorrectStack(i: Int): Unit =
      (if (isSmall(i)) smallIndices else largeIndices).push(i)

    (0 until n) foreach pushIndexInCorrectStack

    for (bucketI <- 0 until n) {
      assert(! smallIndices.empty(),  "Alias method failed for probabilities " +
        unnormalizedProbabilities)
      val smallI = smallIndices.pop()
      leftValues(bucketI) = values(smallI)
      leftProbabilities(bucketI) = remainingProbabilities(smallI) * n
      remainingProbabilities(smallI) = 0.0f
      if (!largeIndices.empty) {
        val largeI = largeIndices.pop()
        rightValues(bucketI) = values(largeI)
        remainingProbabilities(largeI) -= (1.0f - leftProbabilities(bucketI)) / n
        pushIndexInCorrectStack(largeI) // largeI might still be large, or might now be small
      }
    }
    assert(remainingProbabilities forall { _ < 1.0e-5}, "Alias method failed for probabilities "
      + unnormalizedProbabilities + "\nRemaining probabilities " + remainingProbabilities.toSeq)
  }

}

object DiscreteAliasSampler {
  def apply(valueWeights: Map[Int, Float],
           random: Random = new Random()): DiscreteAliasSampler = {
    val (values, probabilities) = valueWeights.toSeq.unzip
    new DiscreteAliasSampler(values, probabilities, random)
  }
}
