package co.teapot.util

/** A resizable array of Ints which supports appending Ints (but not changing or removing current Ints).
  * It supports concurrent reading and writing.
  */
// We store Ints in an array padded with extra capacity that will grow over time
// This is essentially a fastutil IntArrayList, but with synchronization.
class ConcurrentIntArrayList {
  @volatile private var intArray: Array[Int] = new Array[Int](ConcurrentIntArrayList.initialCapacity)
  @volatile private var _size = 0

  def append(ints: Seq[Int]): Unit = {
    this.synchronized {
      if (_size + ints.size > intArray.length) {
        val newCapacity = math.max(
          (intArray.length * ConcurrentIntArrayList.resizeFactor).toInt,
          _size + ints.size)
        val newIntArray = new Array[Int](newCapacity)
        System.arraycopy(intArray, 0, newIntArray, 0, _size)
        intArray = newIntArray
      }
      // Update outgoingArray before updating size, so concurrent reader threads don't read past the end of the array
      for (i <- 0 until ints.size) {
        intArray(i + _size) = ints(i)
      }
      _size = _size + ints.size
    }
  }

  /** Returns an immutable view of the current Ints in this object.
    */
  // Because of volatile references, no lock is needed.
  def toIndexedSeq: IndexedSeq[Int] = {
    // First copy the size, since another thread might increase the size if we copy the intArray
    // reference first, causing size to be longer than intArray.
    val result = new IntArrayView(_size)
    result.intArray = intArray
    result
  }

  def size: Int = _size
}

object ConcurrentIntArrayList {
  val initialCapacity = 2
  val resizeFactor = 2.0
}

/**
  * Stores a reference to an array and a size to create a view of the prefix of the array.  In our use case, the size needs
  * to be set before the intArray to prevent a race condition.
  */
class IntArrayView(override val size: Int) extends IndexedSeq[Int] {
  /** The array this view wraps. */
  var intArray: Array[Int] = null // Should be set after the view has been created
  override def length: Int = size
  override def apply(idx: Int): Int = intArray(idx)
}
