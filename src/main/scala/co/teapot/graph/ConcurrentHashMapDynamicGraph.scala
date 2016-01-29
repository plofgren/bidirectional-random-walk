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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.function.IntUnaryOperator

import co.teapot.util.ConcurrentIntArrayList

import scala.collection.JavaConverters._

/**
 * An efficient dynamic graph implementation which supports concurrent reading and writing.  Locks
  * are only used by writing threads. Nodes are stored in a ConcurrentHashMap.
  * Multi-edges are allowed (multiple copies of an edge may exist in the graph).  Node count and
  * edge count are both available.
 */
class ConcurrentHashMapDynamicGraph extends DynamicDirectedGraph {
  val nodeMap = new ConcurrentHashMap[Int, ConcurrentNode]()
  var _edgeCount = new AtomicLong(0L)
  var _maxNodeId = new AtomicInteger(0)

  override def outDegree(id: Int): Int =
    if (existsNode(id))
      nodeMap.get(id).outDegree
    else
      defaultNeighbors(id).size

  override def edgeCount: Long = _edgeCount.get

  /** Returns the in-neighbors of the given id.
    * Throws NoSuchElementException if id is not a valid id.
    * */
  override def inNeighbors(id: Int): IndexedSeq[Int] =
    if (existsNode(id))
      nodeMap.get(id).inNeighbors
    else
      defaultNeighbors(id)

  override def maxNodeId: Int = _maxNodeId.get

  override def inDegree(id: Int): Int =
    if (existsNode(id))
      nodeMap.get(id).inDegree
    else
      defaultNeighbors(id).size

  override def outNeighbors(id: Int): IndexedSeq[Int] =
    if (existsNode(id))
      nodeMap.get(id).outNeighbors
    else
      defaultNeighbors(id)

  override def nodeIds: Iterable[Int] = new Iterable[Int]() {
    override def iterator: Iterator[Int] = nodeMap.keys().asScala
  }

  override def existsNode(id: Int): Boolean = nodeMap.containsKey(id)

  override def nodeCountOption: Option[Int] = Some(nodeMap.size)

  private def addNode(id: Int): Unit = {
    nodeMap.putIfAbsent(id, new ConcurrentNode())
    _maxNodeId.updateAndGet(new IntUnaryOperator {
      override def applyAsInt(previousMaxId: Int): Int = math.max(previousMaxId, id)
    })
  }

  override def addEdge(id1: Int, id2: Int): Unit = {
    addNode(id1)
    addNode(id2)
    nodeMap.get(id1).addOutBoundNodes(List(id2))
    nodeMap.get(id2).addInBoundNodes(List(id1))
    _edgeCount.incrementAndGet()
  }
}

class ConcurrentNode() {
  val outNeighborList = new ConcurrentIntArrayList()
  val inNeighborList = new ConcurrentIntArrayList()

  def outNeighbors: IndexedSeq[Int] = outNeighborList.toIndexedSeq
  def inNeighbors: IndexedSeq[Int] = inNeighborList.toIndexedSeq

  def addOutBoundNodes(nodeIds: Seq[Int]): Unit =
    outNeighborList.append(nodeIds)
  def addInBoundNodes(nodeIds: Seq[Int]): Unit =
    inNeighborList.append(nodeIds)

  def outDegree: Int = outNeighborList.size
  def inDegree: Int = inNeighborList.size
}

object ConcurrentHashMapDynamicGraph {
  /**
    * Creates a new DynamicGraph from the given file, which is assumed to have lines of the form
    * <id1><whitespace><id2>
    * Any lines with fewer than two tokens are ignored, and any IO or integer parsing exceptions
    * are propagated back to the caller.
    */
  def readGraph(path: String): ConcurrentHashMapDynamicGraph = {
    val graph = new ConcurrentHashMapDynamicGraph()
    for (line <- io.Source.fromFile(path).getLines()) {
      val tokens = line.split("\\s")
      if (tokens.size >= 2) {
        graph.addEdge(tokens(0).toInt, tokens(1).toInt)
      }
    }
    graph
  }

  /**
    * Constructs a ConcurrentHashMapDynamicGraph with the given edges.
    */
  def apply(edges: Iterable[(Int, Int)]): ConcurrentHashMapDynamicGraph = {
    val graph = new ConcurrentHashMapDynamicGraph()
    for ((id1, id2) <- edges) {
        graph.addEdge(id1, id2)
    }
    graph
  }
}
