package soal.util
import collection._
import net.openhft.koloboke.collect.map.hash.{HashIntDoubleMaps, HashIntFloatMaps}
import scala.collection.JavaConverters._

/**
 * Various useful functions on collections.
 */

object CollectionsUtil {
  /**
   * Returns an efficient map, so if we change the primative map library we use we can just
   * change this line of code.
   */
  def efficientIntFloatMap(): mutable.Map[Int, Float] =
    // For some reason, asInstanceOf is still necessary to convert java.lang.Integer to scala.Int
    HashIntFloatMaps.newMutableMap().asScala.asInstanceOf[mutable.Map[Int, Float]]
  // In the future, this might be replaced with a more efficient version
  def efficientIntFloatMapWithDefault0(): mutable.Map[Int, Float] =
    efficientIntFloatMap().withDefaultValue(0.0f)
  def efficientIntDoubleMap(): mutable.Map[Int, Double] =
  // For some reason, asInstanceOf is still necessary to convert java.lang.Integer to scala.Int
    HashIntDoubleMaps.newMutableMap().asScala.asInstanceOf[mutable.Map[Int, Double]]
  def efficientIntDoubleMapWithDefault0(): mutable.Map[Int, Double] =
    efficientIntDoubleMap().withDefaultValue(0.0)

}
