package soal.util

import scala.util.Random

/**
 * A distribution over integers. The only required operations are sampling and expectation
 * computation.
 */

trait DiscreteDistribution {
  def sample(): Int
  def expectation(f: Int => Float): Float
}

class ConstantDistribution(value: Int) extends DiscreteDistribution {
  def sample(): Int = value
  def expectation(f: Int => Float) = f(value)
}

class UniformDistribution(values: IndexedSeq[Int], random: Random) extends DiscreteDistribution {
  def sample(): Int = values(random.nextInt(values.size))
  def expectation(f: Int => Float) =
    (values map f).sum / values.size
}
