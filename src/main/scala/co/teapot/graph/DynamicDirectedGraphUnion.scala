/*
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

/**
 * Wraps a directed graph and a dynamic directed graph to create a dynamic graph with the union of their nodes and edges.  When
 * edge are added, they are added to the underlying dynamic graph.  Edge deletion is not supported.
 */
class DynamicDirectedGraphUnion(staticGraph: DirectedGraph, dynamicGraph: DynamicDirectedGraph)
    extends DynamicDirectedGraph {
  // Because computing nodeCount as an intersection is expensive, maintain nodeCount as a variable.
  private var _nodeCount = if (dynamicGraph.nodeCountOption == Some(0))
    staticGraph.nodeCountOption.get
  else
    (staticGraph.nodeIds.toSet ++ dynamicGraph.nodeIds.toSet).size

  override def outDegree(id: Int): Int = staticGraph.outDegreeOr0(id) + dynamicGraph.outDegreeOr0(id)
  override def inDegree(id: Int): Int = staticGraph.inDegreeOr0(id) + dynamicGraph.inDegreeOr0(id)

  override def outNeighbor(id: Int, i: Int): Int =
    if (i < staticGraph.outDegreeOr0(id))
      staticGraph.outNeighbor(id, i)
    else
      dynamicGraph.outNeighbor(id, i - staticGraph.outDegreeOr0(id))

  override def inNeighbor(id: Int, i: Int): Int =
    if (i < staticGraph.inDegreeOr0(id))
      staticGraph.inNeighbor(id, i)
    else
      dynamicGraph.inNeighbor(id, i - staticGraph.inDegreeOr0(id))

  override def outNeighbors(id: Int): IndexedSeq[Int] =
    (staticGraph.existsNode(id), dynamicGraph.existsNode(id)) match {
      case (false, false) => defaultNeighbors(id)
      case (true, false) => staticGraph.outNeighbors(id)
      case (false, true) => dynamicGraph.outNeighbors(id)
      case (true, true) =>
        new IndexedSeqUnion(staticGraph.outNeighbors(id), dynamicGraph.outNeighbors(id))
    }

  override def inNeighbors(id: Int): IndexedSeq[Int] =
    (staticGraph.existsNode(id), dynamicGraph.existsNode(id)) match {
      case (false, false) => defaultNeighbors(id)
      case (true, false) => staticGraph.inNeighbors(id)
      case (false, true) => dynamicGraph.inNeighbors(id)
      case (true, true) =>
        new IndexedSeqUnion(staticGraph.inNeighbors(id), dynamicGraph.inNeighbors(id))
    }

  override def existsNode(id: Int): Boolean =
    staticGraph.existsNode(id) || dynamicGraph.existsNode(id)

  override def nodeCountOption: Option[Int] = Some(_nodeCount)

  override def nodeIds: Iterable[Int] = {
    val staticGraphIds = staticGraph.nodeIds
    val additionalDynamicGraphIds = dynamicGraph.nodeIds filter (!staticGraph.existsNode(_))
    staticGraphIds ++ additionalDynamicGraphIds
  }

  def maxNodeId: Int = math.max(staticGraph.maxNodeId, dynamicGraph.maxNodeId)

  override def edgeCount: Long = staticGraph.edgeCount + dynamicGraph.edgeCount

  /**
    * Adds the given edge to the underlying dynamic graph. Note that for efficiency we don't check if the edge already exists,
    * so if the edge already exists, a 2nd copy of it will be added.
    */
  override def addEdge(srcId: Int, destId: Int): Unit = {
    if (!existsNode(srcId)) _nodeCount += 1
    if (!existsNode(destId)) _nodeCount += 1
    dynamicGraph.addEdge(srcId, destId)
  }
}

/** Represents the concatenation of two IndexedSeqs. */
private class IndexedSeqUnion[A](xs: Seq[A], ys: Seq[A]) extends IndexedSeq[A] {
  override def length: Int = xs.size + ys.size

  override def apply(i: Int): A =
    if (i < xs.size) {
      xs(i)
    } else if (i - xs.size < ys.size) {
      ys(i - xs.size)
    } else {
      throw new IndexOutOfBoundsException(s"Invalid index $i")
    }
}
