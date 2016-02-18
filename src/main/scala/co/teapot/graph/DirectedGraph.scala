/*
 * Copyright 2014 Twitter, Inc.
 * Copyright 2016 Teapot, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package co.teapot.graph

import scala.util.Random

/**
  * A directed graph where nodes are identified by Int ids.  Implementations may or may not allow
  * multiple copies of the same edge (see their documentation).  If given an invalid node id,
  * methods call the defaultNeighbors method, which by default throws a NoSuchElementException,
  * but this can be overridden, for example to assume that non-existant nodes simply have no
  * neighbors.
  */
trait DirectedGraph {
  /** Returns the number of out-neighbors of the given id.
    * */
  def outDegree(id: Int): Int

  /** Returns the number of in-neighbors of the given id.
    * */
  def inDegree(id: Int): Int

  /** Returns the out-neighbors of the given id.
    * */
  def outNeighbors(id: Int): IndexedSeq[Int]

  /** Returns the in-neighbors of the given id.
    * */
  def inNeighbors(id: Int): IndexedSeq[Int]

  /** Returns the ith out-neighbor of the node with the given id.
    * Throws IndexOutOfBoundsException unless 0 <= i < outDegree(id).
    * */
  /* Subclasses should consider overriding for efficiency to prevent IndexedSeq[Int] objects from
    being created.
   */
  def outNeighbor(id: Int, i: Int): Int = outNeighbors(id)(i)

  /** Returns the ith in-neighbor of the node with the given id.
    * Throws IndexOutOfBoundsException unless 0 <= i < inDegree(id).
    * */
  def inNeighbor(id: Int, i: Int): Int = inNeighbors(id)(i)

  /** All nodeIds that exist in the graph. */
  def nodeIds: Iterable[Int]

  def existsNode(id: Int): Boolean

  /** The largest node id in this graph (or an upper bound on it). */
  def maxNodeId: Int

  /** The number of nodes in this graph.  May return None if it is expensive to compute (for
    * example, for the union of two graphs).*/
  def nodeCountOption: Option[Int]

  /** Returns the nodeCount if it is defined.  Otherwise throws an UnsupportedOperationException.
    */
  def nodeCount: Int = nodeCountOption.getOrElse(
    throw new UnsupportedOperationException("nodeCount not supported on this graph type"))

  /** The number of edges in this graph. */
  def edgeCount: Long

  def uniformRandomOutNeighbor(id: Int, random: Random = Random.self) =
    outNeighbor(id, random.nextInt(outDegree(id)))

  def uniformRandomInNeighbor(id: Int, random:Random = Random.self) =
    inNeighbor(id, random.nextInt(inDegree(id)))

  /** Returns the out-degree of the given node id, or 0 if the node id does not exist. */
  def outDegreeOr0(id: Int): Int =
    if (existsNode(id))
      outDegree(id)
  else
      0

  /** Returns the in-degree of the given node id, or 0 if the node id does not exist. */
  def inDegreeOr0(id: Int): Int =
    if (existsNode(id))
      inDegree(id)
    else
      0
  /** Called by other methods when given an id that doesn't exist in this graph.  By default it
    * throws an exception, but implementations can override it to return an IndexedSeq which is
    * the neighbor seq of non-existing nodes (typically an empty IndexedSeq).
    */
  def defaultNeighbors(id: Int): IndexedSeq[Int] = {
    throw new NoSuchElementException(s"invalid id $id")
  }

  override def toString: String = {
    val result = new StringBuilder()
    result.append(s"Graph ${this.getClass.getName} with $nodeCountOption nodes, max id $maxNodeId, " +
      s"and $edgeCount edges:\n")
    for (id <- nodeIds take 10) {
      result.append(s"\tnode $id has ${outDegree(id)} outNeighbors: " +
        (outNeighbors(id) take 10 mkString ", ") +
        (if (outDegree(id) > 10) "...\n" else "\n"))
    }
    for (id <- nodeIds take 10) {
      result.append(s"\tnode $id has ${inDegree(id)} inNeighbors: " +
        (inNeighbors(id) take 10 mkString ", ") +
        (if (inDegree(id) > 10) "...\n" else "\n"))
    }
    result.toString
  }

  /**
    * Returns the node with the given {@code id} or else {@code None} if the given node does not
    * exist in this graph.  Deprecated: to decrease the number of objects used, get node
    * neighbors and degrees directly through the graph.
    */
  @deprecated("Rather than using Node objects, use graph method directly", "1.0")
  def getNodeById(id: Int): Option[NodeWrapper] =
    if (existsNode(id))
      Some(new NodeWrapper(this, id))
  else
      None
}

object DirectedGraph {
  /** Creates a graph with the given edges. */
  def apply(edges: (Int, Int)*): DirectedGraph =
    DirectedGraph(edges)
  def apply(edges: Iterable[(Int, Int)]): DirectedGraph =
    ConcurrentHashMapDynamicGraph(edges)
}

// Provided for legacy code which uses Node objects
class NodeWrapper(val graph: DirectedGraph, val id: Int) {
  def outboundNodes(): Seq[Int] = graph.outNeighbors(id)
  def inboundNodes(): Seq[Int] = graph.inNeighbors(id)
}
