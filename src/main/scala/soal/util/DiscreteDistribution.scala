package soal.util

import scala.util.Random

/**
 * A distribution over integers. The only required operations are sampling and expectation
 * computation.
 */

trait DiscreteDistribution {
  def sample(): Int
  def valuesAndProbabilities(): Iterable[(Int, Float)]
  def expectation(f: Int => Float): Float =
    (valuesAndProbabilities() map { case (v, p) => p * f(v) }).sum
}

class ConstantDistribution(value: Int) extends DiscreteDistribution {
  def sample(): Int = value
  def valuesAndProbabilities() = List((value, 1.0f))
}

class UniformDistribution(values: IndexedSeq[Int], random: Random) extends DiscreteDistribution {
  def sample(): Int = values(random.nextInt(values.size))
  def valuesAndProbabilities() =
    values zip Array.fill(values.size)(1.0f / values.size)
}
