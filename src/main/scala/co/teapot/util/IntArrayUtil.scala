package co.teapot.util

import java.util

import it.unimi.dsi.fastutil.ints.IntArrayList

object IntArrayUtil {
  def intArrayListToSeq(list: IntArrayList): IndexedSeq[Int] = new IndexedSeq[Int] {
    override def apply(idx: Int): Int = list.getInt(idx)
    override def length: Int = list.size
  }

  /**
    * Returns the distinct elements of the given list, sorted in ascending order.
    */
  def sortedDistinctInts(list: IntArrayList): IntArrayList = {
    val sortedArray: Array[Int] = list.toIntArray
    util.Arrays.sort(sortedArray)

    val result = new IntArrayList(list.size())
    for (i <- 0 until sortedArray.length) {
      if (i == 0 || sortedArray(i - 1) != sortedArray(i)) {
        result.add(sortedArray(i))
      }
    }
    result
  }
}
