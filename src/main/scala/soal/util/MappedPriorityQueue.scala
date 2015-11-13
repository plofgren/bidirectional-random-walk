/*
Copyright 2014 Stanford Social Algorithms Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
  limitations under the License.
*/

package soal.util

import java.util.NoSuchElementException
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.{IntArrayList, Int2IntOpenHashMap}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/** A max prioity queue of Ints with the additional property that it is possible to increase and
  * look-up the priority of elements.
  *
  */
trait MappedIntPriorityQueue {
  def insert(a: Int, priority: Float): Unit
  def contains(a: Int): Boolean
  def increasePriority(a: Int, newPriority: Float): Unit
  def getPriority(a: Int): Float  // Returns 0.0 if a is not in the queue
  def maxPriority: Float
  def extractMax(): Int
  def isEmpty: Boolean
}

/** Standard binary heap, based on Chapter 6 of CLRS Algorithms 2nd Ed.
  * */
class HeapMappedIntPriorityQueue extends MappedIntPriorityQueue {

  private val priorities = new FloatArrayList()
  priorities.push(-1.0f) // The first entry will be ignored to make arithmetic simpler

  private val itemToIndex = new Int2IntOpenHashMap()
  private val indexToItem = new IntArrayList()
  indexToItem.push(-1) // The first entry will be ignored to make arithmetic simpler

  private def parent(i: Int) = i / 2
  private def left(i: Int) = i * 2
  private def right(i: Int) = i * 2 + 1

  private def swap(i: Int, j: Int): Unit = {
    val temp = priorities.get(i)
    priorities.set(i, priorities.get(j))
    priorities.set(j, temp)

    val itemI = indexToItem.getInt(i)
    val itemJ = indexToItem.getInt(j)
    itemToIndex.put(itemI, j)
    itemToIndex.put(itemJ, i)
    indexToItem.set(i, itemJ)
    indexToItem.set(j, itemI)
  }

  /**
  If the max-heap invariant is satisfied except for index i possibly being smaller than a child, restore the invariant.
    */
  private def maxHeapify(i: Int): Unit = {
    var largest = i
    if (left(i) < priorities.size && priorities.get(left(i)) > priorities.get(i)) {
      largest = left(i)
    }
    if (right(i) < priorities.size && priorities.get(right(i)) > priorities.get(largest)) {
      largest = right(i)
    }
    if (largest != i) {
      swap(i, largest)
      maxHeapify(largest)
    }
  }

  override def insert(a: Int, priority: Float): Unit = {
    itemToIndex.put(a, indexToItem.size)
    indexToItem.push(a)
    priorities.push(Float.NegativeInfinity)
    increasePriority(a, priority)
  }

  override def isEmpty: Boolean = {
    indexToItem.size == 1 // first entry is dummy entry
  }

  override def extractMax(): Int = {
    if (isEmpty)
      throw new NoSuchElementException
    val maxItem = indexToItem.get(1)
    swap(1, priorities.size - 1)
    priorities.remove(priorities.size - 1)
    indexToItem.remove(indexToItem.size - 1)
    itemToIndex.remove(maxItem)

    maxHeapify(1)
    maxItem
  }

  override def maxPriority: Float = {
    if (isEmpty)
      throw new NoSuchElementException
    priorities.get(1)
  }

  override def getPriority(a: Int): Float = {
    if (itemToIndex.containsKey(a))
      priorities.get(itemToIndex.get(a))
    else 0.0f // Default priority is 0.0f
  }

  /**
   * Sets the priority of a to newPriority, which must be greater than a's current priority if a is in this queue.
   * Inserts a if it is not already present.
   */
  override def increasePriority(a: Int, newPriority: Float): Unit = {
    if (!contains(a)) {
      insert(a, newPriority)
    } else {
      assert(newPriority >= getPriority(a))
      var i = itemToIndex.get(a)
      priorities.set(i, newPriority)
      while (i > 1 && priorities.get(i) > priorities.get(parent(i))) {
        swap(i, parent(i))
        i = parent(i)
      }
    }
  }

  override def contains(a: Int): Boolean = itemToIndex.containsKey(a)

  def currentPriorities(): collection.Map[Int, Float] = {
    val result = CollectionsUtil.efficientIntFloatMap().withDefaultValue(0.0f)
    for (i <- 1 until priorities.size) {
      result(indexToItem.get(i)) = priorities.get(i)
    }
    result
  }
}