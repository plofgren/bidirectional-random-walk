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
import java.util.Date
import java.util.regex.Pattern

import co.teapot.graph.EdgeDir.EdgeDir
import co.teapot.util.IntArrayUtil
import it.unimi.dsi.fastutil.ints.IntArrayList

import scala.collection.mutable
import scala.io.Source

// Make the constants in MemoryMappedDirectedGraph available
import co.teapot.graph.MemoryMappedDirectedGraph._

/** The convert method converts a graph from a text file of unsorted edges to a binary file which
  * can be efficiently read using MemoryMappedDirectedGraph.  This can also be run from the
  * command line; for example:
  * sbt assembly
  * printf "1 2\n3 4\n1 3" > input_graph.txt
  * java -Xmx7g -cp target/scala-2.11/bidirectional-random-walk-assembly-1.0.jar\
  * co.teapot.graph.MemoryMappedDirectedGraphConverter input_graph.txt output_graph.dat
  *
  * Then in scala code, MemoryMappedDirectedGraph("output_graph.dat") will efficiently read the
  * graph.
  */

object MemoryMappedDirectedGraphConverter {
  /**
    * Converts a text file of lines "<id1><whitespace><id2>" into a binary memory mapped graph.
    * Any duplicate edges are not included in the output.  Empty lines are ignored.
    * Progress is logged using the given function.  See the comment in MemoryMappedDirectedGraph
    * for the interpretation of segmentCount (the default value should work well).
    */
  def convert(
    edgeListFile: File,
    outputGraphFile: File,
    log: String => Unit = System.err.println,
    segmentCountOption: Option[Int] = None ): Unit = {
    new MemoryMappedDirectedGraphConverter(edgeListFile, outputGraphFile, log, segmentCountOption).convert()
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: MemoryMappedDirectedGraphConverter " +
        "<edge list filename> <binary output filename>")
      System.exit(1)
    }
    MemoryMappedDirectedGraphConverter.convert(new File(args(0)), new File(args(1)))
  }
}

class MemoryMappedDirectedGraphConverter(val edgeListFile: File,
                                         val outputGraphFile: File,
                                         val log: String => Unit = System.err.println,
                                         val segmentCountOption: Option[Int] = None ) {
  val InputBytesPerSegment = 1000L * 1000L * 1000L // Segments of 1 GB of input text
  val segmentCount = segmentCountOption.getOrElse(
      (edgeListFile.length() / InputBytesPerSegment).toInt + 1)

  val temporaryFilesById1 = Array.fill(segmentCount)(createTemporaryFile())
  val temporaryFilesById2 = Array.fill(segmentCount)(createTemporaryFile())

  // These four variables will be modified after data is read
  var nodeCount = 0
  var edgesReadCount = 0L
  var distinctEdgeCount = 0L
  val segmentSizes = mutable.Map[EdgeDir, Array[Int]](
    EdgeDir.Out -> Array.fill(segmentCount)(0),
    EdgeDir.In -> Array.fill(segmentCount)(0))

  private def nodesPerSegment: Int = nodeCount / segmentCount

  def convert(): Unit = {
    partitionEdgesAmongTemporaryFiles()
    convertSegmentsToBinary()
    writeHeader()
    cleanUp()
  }

  private def partitionEdgesAmongTemporaryFiles(): Unit = {
    val outStreamsById1 = temporaryFilesById1 map createOutputStream
    val outStreamsById2 = temporaryFilesById2 map createOutputStream
    var maxNodeId = 0

    forEachEdge(edgeListFile) { (id1, id2) =>
      maxNodeId = math.max(maxNodeId, math.max(id1, id2))
      val outStreamById1 = outStreamsById1(id1 % segmentCount)
      outStreamById1.writeInt(id1)
      outStreamById1.writeInt(id2)
      val outStreamById2 = outStreamsById2(id2 % segmentCount)
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
    // For convenience, round nodeCount up to the next multiple of segmentCount
    nodeCount = nextMultiple(maxNodeId + 1, segmentCount)
  }

  private def writeHeader(): Unit = {
    val outputStream = new RandomAccessFile(outputGraphFile, "rw")
    outputStream.writeLong(Version)
    outputStream.writeInt(nodeCount)
    outputStream.writeLong(distinctEdgeCount)
    outputStream.writeInt(segmentCount)
    var segmentOffset = OffsetToSegmentOffsets + (segmentCount + 1) * BytesPerSegmentOffset * 2
    for (dir <- Seq(EdgeDir.Out, EdgeDir.In)) {
      for (segmentI <- 0 until segmentCount) {
        outputStream.writeLong(segmentOffset)
        segmentOffset += segmentSizes(dir)(segmentI)
      }
      outputStream.writeLong(segmentOffset) // For computing the size of the last segment
    }
    outputStream.close()
  }

  /**
    * For each segment, for outNeighbors and inNeighbors,
    * reads the edges of that segment from a temporary file into arrays, grouped by id.  Then calls
    * writeEdgeDataToStream to write the edges to output
    */
  private def convertSegmentsToBinary(): Unit = {
    val outputStream = createOutputStream(outputGraphFile)
    val headerSize = OffsetToSegmentOffsets + (segmentCount + 1) * 2 * BytesPerSegmentOffset
    // Skip over header (which we will write later)
    outputStream.write(Array.fill(headerSize)(0: Byte))

    for (dir <- Seq(EdgeDir.Out, EdgeDir.In)) {
      for (segmentI <- 0 until segmentCount) {
        val neighborArrayLists = Array.fill(nodesPerSegment)(new IntArrayList())
        val inStream = createInputStream(temporaryFilesInDirection(dir)(segmentI))
        val tempFileEdgeCount = (temporaryFilesInDirection(dir)(segmentI).length / 8).toInt
        for (edgeIndex <- 0 until tempFileEdgeCount) {
          val id1 = inStream.readInt()
          val id2 = inStream.readInt()
          dir match {
            case EdgeDir.Out => neighborArrayLists(id1 / segmentCount).add(id2)
            case EdgeDir.In => neighborArrayLists(id2 / segmentCount).add(id1)
          }
        }
        inStream.close()

        val distinctNeighborArrayLists = neighborArrayLists map IntArrayUtil.sortedDistinctInts
        writeEdgeDataToStream(distinctNeighborArrayLists, outputStream)

        // Store segment size
        val edgeCount = (distinctNeighborArrayLists map { _.size }).sum
        if (dir == EdgeDir.Out)
          distinctEdgeCount += edgeCount
        val segmentSize = (nodesPerSegment + 1) * BytesPerNodeOffset + edgeCount * BytesPerNeighbor
        require(segmentSize < Integer.MAX_VALUE, "Segment too large for Int indexing")
        segmentSizes(dir)(segmentI) = segmentSize
        log(s"wrote ${segmentI + 1} of $segmentCount $dir-segments.")
      }
    }
    outputStream.close()
  }

  /**
    * Given an array of neighbor arrays for a segment, first computes and writes offsets (relative
    * to the start of this segment), then writes the neigbhor data.
    */
  private def writeEdgeDataToStream(neighborArrayLists: Array[IntArrayList], out: DataOutputStream): Unit = {
    var offset = 0
    for (neighborList <- neighborArrayLists) {
      out.writeInt(offset)
      offset += neighborList.size * BytesPerNeighbor
    }
    out.writeInt(offset) // Write extra offset for computing degree of last node

    for (neighborList <- neighborArrayLists) {
      for (j <- 0 until neighborList.size) {
        out.writeInt(neighborList.get(j))
      }
    }
  }

  private def cleanUp(): Unit = {
    temporaryFilesById1 foreach (_.delete())
    temporaryFilesById2 foreach (_.delete())
  }

  private def temporaryFilesInDirection(d: EdgeDir): Array[File] = d match {
    case EdgeDir.Out => temporaryFilesById1
    case EdgeDir.In => temporaryFilesById2
  }

  private def createTemporaryFile(): File = {
    val newFile = File.createTempFile("edge_partition", ".bin")
    newFile.deleteOnExit() // Request file gets deleted in case an exception happens
    newFile
  }

  private def createOutputStream(f: File): DataOutputStream =
    new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)))

  private def createInputStream(f: File): DataInputStream =
    new DataInputStream(new BufferedInputStream(new FileInputStream(f)))

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

  /** Given positive integers n and k, returns a positive integer n' such that n' is a multiple
    * of k and n <= n' < n + k. Assumes n + k < pow(2, 31). */
  private def nextMultiple(n: Int, k: Int): Int =
    (n + k - 1) / k * k
}

object EdgeDir extends Enumeration {
  type EdgeDir = Value
  val Out, In = Value
}