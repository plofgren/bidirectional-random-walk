package co.teapot.graph

import scala.collection.mutable

/**
 * Wraps a sequence of directed graphs to create a view of the union of their nodes and edges.
 * Graph mutations are not supported through this view, but if underlying graphs change, this view
 * will reflect the changes.
 */
class DirectedGraphUnion(graphs: Seq[DirectedGraph])
  extends DirectedGraph {

  override def outDegree(id: Int): Int = (graphs map (_.outDegree(id))).sum
  override def inDegree(id: Int): Int = (graphs map (_.inDegree(id))).sum

  override def outNeighbors(id: Int): IndexedSeq[Int] =
    new FlattenedIndexedSeq(graphs filter (_.existsNode(id)) map (_.outNeighbors(id)))

  override def inNeighbors(id: Int): IndexedSeq[Int] =
    new FlattenedIndexedSeq(graphs filter (_.existsNode(id)) map (_.inNeighbors(id)))

  override def outNeighbor(id: Int, i: Int): Int = {
    var adjustedI = i // i - (cumulative outdegree of id in previous graphs)
    for (graph <- graphs) {
      val outDegree = graph.outDegree(id)
      if (adjustedI < outDegree) {
        return graph.outNeighbor(id, adjustedI)
      } else {
        adjustedI -= outDegree
      }
    }
    throw new IndexOutOfBoundsException(s"index $i invalid for out-neighbor of $id")
  }

  override def inNeighbor(id: Int, i: Int): Int = {
    var adjustedI = i // i - (cumulative indegree of id in previous graphs)
    for (graph <- graphs) {
      val inDegree = graph.inDegree(id)
      if (adjustedI < inDegree) {
        return graph.inNeighbor(id, adjustedI)
      } else {
        adjustedI -= inDegree
      }
    }
    throw new IndexOutOfBoundsException(s"index $i invalid for in-neighbor of $id")
  }

  override def edgeCount: Long = (graphs map (_.edgeCount)).sum

  /** Since the underlying graphs could change, it is not clear how to implement nodeCount without
    * iterating over all nodes in all graphs.
    */
  override def nodeCountOption: Option[Int] = None

  override def maxNodeId: Int = (graphs map (_.maxNodeId)).max

  override def existsNode(id: Int): Boolean =
    graphs exists { _.existsNode(id) }

  override def nodeIds: Iterable[Int] =
    (0 to maxNodeId) filter { id => graphs forall (_.existsNode(id)) }

  override def toString() =
    s"A union of ${graphs.size} graphs: $graphs"
}

/** Represents the concatenation of 0 or more IndexedSeqs (without the overhead of physically
  * concatanating them).
  * Indexing time is O(k) for k sequences,
  *  so for a large number of sequences a different class based on trees or binary search should be
  *  used.
  *  */
// Note: we assume each sequence has efficient random access; if not, the apply method will be slow.
// It would be natural for seqs to have type Seq[IndexedSeq[Int]], but currently Node.outNeighbors
// returns a Seq[Int], not an IndexedSeq[Int].
private class FlattenedIndexedSeq[A](seqs: Seq[Seq[A]]) extends IndexedSeq[A] {
  override def length: Int = (seqs map (_.size)).sum

  override def apply(i: Int): A = {
    // We do a linear search for simplicity
    var skippedElementCount = 0
    for (seq <- seqs) {
      if (i - skippedElementCount < seq.size) {
        return seq(i - skippedElementCount)
      }
      skippedElementCount += seq.size
    }
    throw new IndexOutOfBoundsException(s"Invalid index $i to IndexedSeqUnion with underlying seq" +
      s" lengths ${seqs map (_.size)}")
  }

  // We override for efficiency.
  override def foreach[U](f: A => U): Unit = {
    seqs foreach { seq =>
      seq foreach f
    }
  }
}