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

import java.io._
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption

import co.teapot.graph.EdgeDir.EdgeDir
import co.teapot.io.ByteBufferIntSlice

// Make constants available
import co.teapot.graph.MemoryMappedDirectedGraphConstants._

/**
 * A graph which reads edge data from a memory mapped file.  There is no object overhead per
 * node: the memory used for with both in-neighbor and out-neighbor access is 8 bytes per edge
  * and 8 bytes per node. Loading is very fast because no parsing of text is required.
 * Loading time is exactly the time it takes the operating system to page data from disk into
 * memory.  Nodes are numbered sequentially from 0 to nodeCount - 1 and must be a range of this
 * form (i.e. nodeCount == maxNodeId + 1).
 *
 * Graphs are converted to this format using MemoryMappedDirectedGraphConverter, then loaded
  * using this class.  When transforming a graph where nodeCount <= maxNodeId
 * to this format, new nodes with no neighbors will be implicitly created. The binary
 * format is currently subject to change.
 */

/* Internally, the graph is stored in a large binary file made of a header and a series of segments.
Out-neighbor segment i contains the out-neighbors of all nodes whose id satisfies
  id % segmentCount == i
and similarly for in-neighbor segment i.  Each segment has a sequence of offsets, where the jth
offset in out-neighbor segment i stores the offset (as an int relative to the start of that segment) for
node
  id = j * segmentCount + i
where nodesPerSegment = nodeCount / segmentCount.  We require for simplicity that nodeCount be
a multiple of segmentCount (by creating empty nodes if needed).

Storage format
Header:
byteCount  data
8          (version number, currently 1)
4          n (nodeCount).  Must be a multiple of segmentCount.
8          edgeCount
4          k (segmentCount)
8*(k + 1)  The absolute offset in bytes where each out-neighbor segment's data begins.  The "+ 1"
           is for computing the size of the last segment.
8*(k + 1)  (Similarly, for in-neighbors)

Each out-neighbor segment:
byteCount  data
4*(n/k+1)  Offset in Ints into out-neighbor data relative to the start of this segment. Index j
           plus the offset of this segment points to the out-neighbor data of node
           id = (j * segmentCount + i).  The
           out-neighbor data is be stored in increasing order by id, so the outegree of node
           id is computed from the difference in offset between node id + segmentCount and node id.
           One additional index is needed to compute the outdegree of the last node in this segment.
4*(# edges in segment)        out-neighbor data

Each in-neighbor segment is analogous to an out-neighbor segment.
 */
class MemoryMappedDirectedGraph(file: File) extends DirectedGraph {
  private val fileChannel = FileChannel.open(file.toPath, StandardOpenOption.READ)

  private val headerData = memoryMapHeader()
  require(headerData.getLong(OffsetToVersion) == Version,
    s"Invalid graph version ${headerData.getLong(OffsetToVersion)}")
  override val nodeCount = headerData.getInt(OffsetToNodeCount)
  override val edgeCount = headerData.getLong(OffsetToEdgeCount)
  private val segmentCount = headerData.getInt(OffsetToSegmentCount)

  private val outNeighborSegments: Array[Segment] = memoryMapSegments(EdgeDir.Out)
  private val inNeighborSegments: Array[Segment] = memoryMapSegments(EdgeDir.In)

  override def maxNodeId = nodeCount - 1

  override def existsNode(id: Int): Boolean = 0 <= id && id <= maxNodeId

  override def nodeCountOption: Option[Int] = Some(nodeCount)

  override def outDegree(id: Int): Int =
    if (existsNode(id))
      outNeighborSegments(id % segmentCount).degree(id / segmentCount)
    else
      defaultNeighbors(id).size

  override def inDegree(id: Int): Int =
    if (existsNode(id))
      inNeighborSegments(id % segmentCount).degree(id / segmentCount)
    else
      defaultNeighbors(id).size

  def outNeighbors(id: Int): IndexedSeq[Int] =
    if (existsNode(id))
      outNeighborSegments(id % segmentCount).neighbors(id / segmentCount)
    else
      defaultNeighbors(id)

  def inNeighbors(id: Int): IndexedSeq[Int] =
    if (existsNode(id))
      inNeighborSegments(id % segmentCount).neighbors(id / segmentCount)
    else
      defaultNeighbors(id)

  override def outNeighbor(id: Int, i: Int): Int =
    if (0 <= i && i < outDegree(id))
      outNeighborSegments(id % segmentCount).neighbor(id / segmentCount, i)
    else
      throw new IndexOutOfBoundsException(
        s"invalid index $i to node $id with outDegree ${outDegree(id)}")

  override def inNeighbor(id: Int, i: Int): Int =
    if (0 <= i && i < inDegree(id))
      inNeighborSegments(id % segmentCount).neighbor(id / segmentCount, i)
    else
      throw new IndexOutOfBoundsException(
        s"invalid index $i to node $id with inDegree ${inDegree(id)}")

  def nodeIds: Iterable[Int] = 0 to maxNodeId

  /** Loads the graph data into physical RAM, so later graph operations don't have lag.  Makes a
    * "best effort" (see MappedByteBuffer.load()).
    */
  def preloadToRAM(): Unit = {
    for (segment <- outNeighborSegments ++ inNeighborSegments) {
      segment.data.buffer.load()
    }
  }

  private def memoryMapHeader(): MappedByteBuffer = {
    // First determine size
    val headerHeader = fileChannel.map(MapMode.READ_ONLY, 0, OffsetToSegmentOffsets)
    val segmentCount = headerHeader.getInt(OffsetToSegmentCount)
    val headerSize = OffsetToSegmentOffsets + (segmentCount + 1) * 2 * 8

    fileChannel.map(MapMode.READ_ONLY, 0, headerSize)
  }

  /** Loads segments into memory-mapped buffers. */
  private def memoryMapSegments(dir: EdgeDir): (Array[Segment]) = {
    (0 until segmentCount).toArray map { i =>
      val baseOffset = dir match {
        case EdgeDir.Out => OffsetToSegmentOffsets
        case EdgeDir.In => OffsetToSegmentOffsets + (segmentCount + 1) * BytesPerSegmentOffset
      }
      val offset = headerData.getLong(baseOffset + i * BytesPerSegmentOffset)
      val size = headerData.getLong(baseOffset + (i + 1) * BytesPerSegmentOffset) - offset
      val buffer = fileChannel.map(MapMode.READ_ONLY,  offset, size)
      new Segment(new ByteBufferIntSlice(buffer, 0, size.toInt / BytesPerInt))
    }
  }
}

/* Reads data for a segment of nodes. Methods are in terms of node index i = id /
segmentCount, where id is the id of the node in the overall graph.  The same segment class is
used for out-neighbors and in-neighbors.
 */
private[graph] class Segment(val data: ByteBufferIntSlice) {
  /** Returns the offset (in Ints, not Bytes) where the ith node's neighbor data starts. */
  def offset(i: Int): Int =
    data(i)

  /** Returns the degree of the ith node in this segment. */
  def degree(i: Int): Int =
    offset(i + 1) - offset(i)

  /** Returns the jth neighbor of the i-th node in this segment. */
  def neighbor(i: Int, j: Int): Int =
    data(offset(i) + j)

  /** Returns a view of the neighbors of the i-th node in this segment. */
  def neighbors(i: Int): IndexedSeq[Int] =
    data.subSlice(offset(i), degree(i))
}

object MemoryMappedDirectedGraph {
  def apply(file: File): MemoryMappedDirectedGraph =
    new MemoryMappedDirectedGraph(file)

  def apply(filename: String): MemoryMappedDirectedGraph =
    new MemoryMappedDirectedGraph(new File(filename))
}

object MemoryMappedDirectedGraphConstants {
// Constants related to the binary format
  val OffsetToVersion = 0
  val OffsetToNodeCount = 8
  val OffsetToEdgeCount = 12
  val OffsetToSegmentCount = 20
  val OffsetToSegmentOffsets = 24

  val BytesPerSegmentOffset = 8
  val BytesPerNodeOffset = 4 // Size of offsets per node within each segment
  val BytesPerNeighbor = 4
  val BytesPerInt = 4

  val Version = 1L
}
