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
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.Date
import java.util.regex.Pattern

import MemoryMappedDirectedGraph.{BytesPerNeighbor, BytesPerOffset, HeaderSize}
import co.teapot.io.{IntSourceSlice, MemoryMappedIntLongSource}
import co.teapot.util.IntArrayUtil
import it.unimi.dsi.fastutil.ints.IntArrayList

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

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
class MemoryMappedDirectedGraph(file: File) extends DirectedGraph {
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

object MemoryMappedDirectedGraph {
  val HeaderSize = 16L // 8 reserved bytes and 8 bytes for nodeCount
  val BytesPerOffset = 8L
  val BytesPerNeighbor = 4L

  val defaultNodesPerChunk = 250 * 1000

  /** Writes the given graph to the given file (overwriting it if it exists) in the current binary
   * format.
   */
  def graphToFile(graph: DirectedGraph, file: File): Unit = {

    val n = graph.maxNodeId + 1 // includes both 0 and maxNodeId as ids
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))
    out.writeLong(0)
    out.writeLong(n)
    //The outneighbor data starts after the initial 8 bytes, n+1 Longs for outneighbors, and n+1
    // Longs for in-neighbors
    var outboundOffset = HeaderSize + BytesPerOffset * (n + 1) * 2
    for (i <- 0 until n) {
      out.writeLong(outboundOffset)
      if (graph.existsNode(i))
        outboundOffset += BytesPerNeighbor * graph.outDegree(i)
    }
    out.writeLong(outboundOffset) // Needed to compute out-degree of node n-1

    // The inbound data starts immediately after the outbound data
    var inboundOffset = outboundOffset
    for (i <- 0 until n) {
      out.writeLong(inboundOffset)
      if (graph.existsNode(i))
        inboundOffset += BytesPerNeighbor * graph.inDegree(i)
    }
    out.writeLong(inboundOffset) // Needed to compute in-degree of node n-1

    for (i <- 0 until n
         if graph.existsNode(i)) {
      for (v <- graph.outNeighbors(i)) {
        out.writeInt(v)
      }
    }
    for (i <- 0 until n
         if graph.existsNode(i)) {
      for (v <- graph.inNeighbors(i)) {
        out.writeInt(v)
      }
    }
    out.close()
  }

  /** Parses each line of the given file, assumed to each have the form "<int><whitespace><int>"
    * and calls the given function of each pair of ints.  Empty lines are ignored.
    */
  private def forEachEdge(edgeListFile: File)(f: (Int, Int) => Unit): Unit = {
    val linePattern = Pattern.compile(raw"(\d+)\s+(\d+)")
    for (line <- Source.fromFile(edgeListFile).getLines()
         if line.nonEmpty) {
      val matcher = linePattern.matcher(line)
      if (!matcher.matches()) {
        throw new IOException("invalid line in edge file: " + line)
      } else {
        val u = matcher.group(1).toInt // Groups are 1-indexed
        val v = matcher.group(2).toInt
        f(u, v)
      }
    }
  }

  /** Helper method to edgeFileToGraph.
    * Distributes the edges in edgeListFile among a buffer of files, which will be added to
    * tempFilesById1 and tempFilesById2. Also computes and returns maxNodeId.
    */
  private def partitionEdgesAndReturnTempFilesAndMaxNodeId(
      edgeListFile: File,
      nodesPerChunk: Int,
      log: String => Unit): (mutable.IndexedSeq[File], mutable.IndexedSeq[File], Int) = {
    val tempFilesById1 = new ArrayBuffer[File]()
    val tempFilesById2 = new ArrayBuffer[File]()
    val outStreamsById1 = new ArrayBuffer[DataOutputStream]()
    val outStreamsById2 = new ArrayBuffer[DataOutputStream]()

    var edgesReadCount = 0L // Only needed for logging
    var maxNodeId = 0

    log("started reading graph at " + new Date())
    forEachEdge(edgeListFile) { (id1, id2) =>
      maxNodeId = math.max(maxNodeId, math.max(id1, id2))
      // Increase the number of temp files if needed
      while (maxNodeId >= tempFilesById1.size * nodesPerChunk) {
        createAndStoreNewTemporaryFile(tempFilesById1, outStreamsById1)
        createAndStoreNewTemporaryFile(tempFilesById2, outStreamsById2)
      }

      val outStreamById1 = outStreamsById1(id1 / nodesPerChunk)
      outStreamById1.writeInt(id1)
      outStreamById1.writeInt(id2)
      val outStreamById2 = outStreamsById2(id2 / nodesPerChunk)
      outStreamById2.writeInt(id1)
      outStreamById2.writeInt(id2)
      edgesReadCount += 1
      if (edgesReadCount % (100 * 1000 * 1000) == 0) {
        log(s"read $edgesReadCount edges")
      }
    }

    outStreamsById1 foreach (_.close())
    outStreamsById2 foreach (_.close())
    log(s"read $edgesReadCount total edges (including any duplicates)")
    log("finished first pass at " + new Date())
    (tempFilesById1, tempFilesById2, maxNodeId)
  }

  /** Helper method to edgeFileToGraph.
    * Creates a new temporary file, appends it to fileBuffer, and appends a buffered output
    * stream to outputStreamBuffer.
    */
  private def createAndStoreNewTemporaryFile(fileBuffer: mutable.Buffer[File],
                                     outputStreamBuffer: mutable.Buffer[DataOutputStream]): Unit = {
    val newFile = File.createTempFile("edge_partition", ".bin")
    fileBuffer += newFile
    outputStreamBuffer += new DataOutputStream(
      new BufferedOutputStream(new FileOutputStream(newFile)))
    newFile.deleteOnExit() // Request file gets deleted in case an exception happens
  }

  /** Helper method to edgeFileToGraph.
    * Reads edges from the given file and stores them in adjacency lists, where the ith list
    * contains the neighbors of the ith node (where indexing 0 corresponds to nodeId
    * chunkIndex * nodesPerChunk)
    */
  def edgeFileToNeighborArrayLists(
      tempFile: File,
      nodeCount: Int,
      chunkIndex: Int,
      nodesPerChunk: Int,
      neighborType: GraphDir.Value): Array[IntArrayList] = {

    // Read edges from temporary file into arrays
    val inStream = new DataInputStream(new BufferedInputStream(
      new FileInputStream(tempFile)))

    // The last chunk might not have the full number of nodes, so compute # nodes in this chunk.
    val nodeCountInChunk = math.min(nodesPerChunk, nodeCount - chunkIndex * nodesPerChunk)
    val neighborArrays = Array.fill(nodeCountInChunk)(new IntArrayList())
    // The difference between a nodeId and the corresponding index into neighborArrays
    val nodeOffset = chunkIndex * nodesPerChunk
    val tempFileEdgeCount = tempFile.length() / 8L
    for (edgeIndex <- Stream.range(0L, tempFileEdgeCount)) {
      val id1 = inStream.readInt()
      val id2 = inStream.readInt()
      neighborType match {
        case GraphDir.Out => neighborArrays(id1 - nodeOffset).add(id2)
        case GraphDir.In => neighborArrays(id2 - nodeOffset).add(id1)
      }
    }
    inStream.close()
    neighborArrays
  }

  /** Helper method to edgeFileToGraph.
    * Appends neighbor data to binaryGraphOutput, and writes the offsets to binaryGraphChannel at
    * chunkOffsetsStart.
    */
  def writeNeighborDataToGraphAndReturnOffset(
      neighborLists: IndexedSeq[IntArrayList],
      initialCumulativeNeighborOffset: Long,
      binaryGraphOutput: DataOutputStream,
      binaryGraphChannel: FileChannel,
      chunkOffsetsStart: Long,
      isLastChunk: Boolean): Long = {
    var cumulativeNeighborOffset = initialCumulativeNeighborOffset
    val neighborOffsets = new Array[Long](neighborLists.size)
    for ((neighbors, i) <- neighborLists.zipWithIndex) {
      neighborOffsets(i) = cumulativeNeighborOffset
      val sortedDistinctNeighbors = IntArrayUtil.sortedDistinctInts(neighbors)
      for (j <- 0 until sortedDistinctNeighbors.size()) {
        val neighborId = sortedDistinctNeighbors.get(j)
        // binaryGraphOuput is buffered, so the writeInt calls are efficient
        binaryGraphOutput.writeInt(neighborId)
        cumulativeNeighborOffset += 4L
      }
    }
    // Store the ending offset for the last node
    val finalOffset = cumulativeNeighborOffset
    binaryGraphOutput.flush()


    val offsetsBuffer = binaryGraphChannel.map(MapMode.READ_WRITE, chunkOffsetsStart,
      8 * (neighborLists.size + 1)) // The + 1 is for the edge case described below

    for (offset <- neighborOffsets) {
      offsetsBuffer.putLong(offset)
    }
    // Edge case: For the last chunk, we need to write the final offset, in order to store
    // the nth node's out-degree (and in-degree).
    if (isLastChunk) {
      offsetsBuffer.putLong(finalOffset)
    }
    offsetsBuffer.force()  // Make sure data in memory map is copied to disk
    cumulativeNeighborOffset
  }

  /** Converts a graph to binary format.  The input is a file containing lines  of the form
    * "<id1> <id2>", and this method will throw an IOException if any non-blank line of the file
    * doesn't have this form.
    * The graph in binary format is written to the given file.  This method works by splitting
    * edges into chunks with contiguous id1 (and also chunks with contiguous id2).  Duplicate edges
    * are omitted from the output graph.
    *
    * For performance, the parameter nodesPerChunk can be tuned.  It sets the number of nodes in
    * each temporary file, for example if nodesPerChunk is 10^6, then node ids 0..(10^6-1) will
    * be in the first chunk, ids 10^6..(2*10^6-1) will be in the second chunk, etc.  The amount
    * of RAM used is proportional to the largest total number of edges incident on nodes in any
    * single chunk.
    *
    * If logging function (e.g. System.err.println) is given, progress will be logged there.
    *
    * Removes any duplicate edges in the input file.
    * */
  def edgeFileToGraph(edgeListFile: File,
                      graphFile: File,
                      nodesPerChunk: Int = defaultNodesPerChunk,
                      log: String => Unit = (x => Unit)): Unit = {

    val (tempFilesById1, tempFilesById2, maxNodeId) =
      partitionEdgesAndReturnTempFilesAndMaxNodeId(
        edgeListFile, nodesPerChunk, log)

    val nodeCount = maxNodeId + 1

    // Our second pass will alternate between appending new neighbor data at the end of the file,
    // and filling in node neighbor offsets near the beginnning of the file.  A RandomAccessFile
    // was found to be too slow (because it lacks buffering), so we will use a
    // BufferedOutputStream for appending neighbor data, and a FileChannel for writing the
    // neighbor offsets.
    val binaryGraphChannel = FileChannel.open(graphFile.toPath,
      StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    val binaryGraphOutput = new DataOutputStream(
      new BufferedOutputStream(new FileOutputStream(graphFile)))
    binaryGraphOutput.writeLong(0) // Reserved bytes
    binaryGraphOutput.writeLong(nodeCount.toLong)
    // Skip past the node offsets; they will be filled in later using binaryGraphChannel.
    for (i <- Stream.range(0,  2L * (nodeCount + 1))) {
      binaryGraphOutput.writeLong(0)
    }
    binaryGraphOutput.flush()

    // cumulativeNeighborOffset is the byte offset where neighbor data should be written next.
    // The out-neighbor data starts after the initial 16 bytes, n+1 Longs for out-neighbors and n+1
    // Longs for in-neighbors.
    val firstNeighborOffset = HeaderSize + BytesPerOffset * (nodeCount + 1) * 2L
    var cumulativeNeighborOffset = firstNeighborOffset

    // To prevent duplicated code between out-neighbor and in-neighbor writing, iterate over the
    // neighbor types we write (first Out, then In).
    for (neighborType <- List(GraphDir.Out, GraphDir.In)) {
      val tempFiles = neighborType match {
        case GraphDir.Out => tempFilesById1
        case GraphDir.In => tempFilesById2
      }
      for ((tempFile, chunkIndex) <- tempFiles.zipWithIndex) {
        val neighborLists = edgeFileToNeighborArrayLists(
          tempFile, nodeCount, chunkIndex, nodesPerChunk, neighborType)

        // Location in output file where the
        val chunkOffsetsStart = neighborType match {
          case GraphDir.Out => HeaderSize + BytesPerOffset * chunkIndex * nodesPerChunk
          case GraphDir.In =>
            HeaderSize + BytesPerOffset * chunkIndex * nodesPerChunk + BytesPerOffset * (nodeCount + 1)
        }
        cumulativeNeighborOffset = writeNeighborDataToGraphAndReturnOffset(
          neighborLists,
          cumulativeNeighborOffset,
          binaryGraphOutput,
          binaryGraphChannel,
          chunkOffsetsStart,
          isLastChunk = nodesPerChunk * (chunkIndex + 1) >= nodeCount)
        log(s"wrote ${(cumulativeNeighborOffset - firstNeighborOffset) / 4} half edges")
      }
    }

    binaryGraphOutput.close()
    binaryGraphChannel.close()
    tempFilesById1 foreach (_.delete())
    tempFilesById2 foreach (_.delete())
    log("finished writing graph at " + new Date())
  }
}

object GraphDir extends Enumeration {
  type GraphDir = Value
  val Out, In = Value
}
