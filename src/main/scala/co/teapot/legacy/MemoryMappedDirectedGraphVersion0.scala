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

package co.teapot.legacy

import java.io._

import co.teapot.graph.DirectedGraph
import MemoryMappedDirectedGraphVersion0._
/**
 * A graph which reads edge data from a memory mapped file.  There is no object overhead per
 * node: the memory used for n nodes and m edges with both in-neighbor and out-neighbor access is
 * exactly 16 + 16*n + 8*m bytes. Also, loading is very fast because no parsing of text is required.
 * Loading time is exactly the time it takes the operating system to page data from disk into
 * memory.  Nodes are numbered sequentially from 0 to nodeCount - 1 and must be a range of this
 * form (i.e. nodeCount == maxNodeId + 1).
 *
 * When transforming a graph where nodeCount <= maxNodeId
 * to this format, new nodes with no neighbors will be implicitly created. The binary
 * format is currently subject to change.
 */

/* Storage format
byteCount  data
8          (reserved, later use for versioning or indicating undirected vs directed)
8          n (i. e. the number of nodes).  Currently must be less than 2^31.
8*(n+1)    Offsets into out-neighbor data. Index i (a Long) points to the out-neighbor data of
           node i.  The out-neighbor data must be stored in sequential order by id, as the
           outegree of node i is computed from the difference in offset between node i+1 and node i.
           Index n is needed to compute the outdegree of node n - 1.
8*(n+1)    Offsets into in-neighbor data (Longs) (Same interpretation as out-neighbor offsets)
4*m        out-neighbor data
4*m        in-neighbor data
 */
class MemoryMappedDirectedGraphVersion0(file: File) extends DirectedGraph {
  val data: MemoryMappedIntLongSource = new MemoryMappedIntLongSource(file)

  // In the future we may want to support Long ids, so store nodeCount as Long
  override val nodeCount = data.getLong(8).toInt

  override def maxNodeId = nodeCount - 1

  override def existsNode(id: Int): Boolean = 0 <= id && id <= maxNodeId

  override def nodeCountOption: Option[Int] = Some(nodeCount)

  /** The offset into data where the first out-neighbor of the given id is stored.
    * */
  private def outboundOffset(id: Int): Long =
    data.getLong(HeaderSize + BytesPerOffset * id)

  /** The offset into data where the first in-neighbor of the given id is stored.
    * */
  private def inboundOffset(id: Int): Long =
    data.getLong(HeaderSize + BytesPerOffset * (nodeCount + 1) + BytesPerOffset * id)

  override def outDegree(id: Int): Int =
    if (existsNode(id))
      ((outboundOffset(id + 1) - outboundOffset(id)) / BytesPerNeighbor).toInt
    else
      defaultNeighbors(id).size

  override def inDegree(id: Int): Int =
    if (existsNode(id))
      ((inboundOffset(id + 1) - inboundOffset(id)) / BytesPerNeighbor).toInt
    else
      defaultNeighbors(id).size

  def outNeighbors(id: Int): IndexedSeq[Int] =
    new IntSourceSlice(data, outboundOffset(id), outDegree(id))

  def inNeighbors(id: Int): IndexedSeq[Int] =
    new IntSourceSlice(data, inboundOffset(id), inDegree(id))

  override def outNeighbor(id: Int, i: Int): Int =
    if (0 <= i && i < outDegree(i))
      data.getInt(outboundOffset(id) + BytesPerNeighbor * i)
    else
      throw new IndexOutOfBoundsException(
        s"invalid index $i to node $id with outDegree ${outDegree(id)}")

  override def inNeighbor(id: Int, i: Int): Int =
    if (0 <= i && i < inDegree(i))
      data.getInt(inboundOffset(id) + BytesPerNeighbor * i)
    else
      throw new IndexOutOfBoundsException(
        s"invalid index $i to node $id with inDegree ${inDegree(id)}")

  def nodeIds: Iterable[Int] = 0 to maxNodeId

  def edgeCount: Long = (outboundOffset(nodeCount) - outboundOffset(0)) / BytesPerNeighbor

  /** Loads the graph data into physical RAM, so later graph operations don't have lag.  Makes a
    * "best effort" (see MappedByteBuffer.load()).
    */
  def preloadToRAM(): Unit = {
    data.loadFileToRam()
  }
}

object MemoryMappedDirectedGraphVersion0 {
  val HeaderSize = 16L // 8 reserved bytes and 8 bytes for nodeCount
  val BytesPerOffset = 8L
  val BytesPerNeighbor = 4L
}
