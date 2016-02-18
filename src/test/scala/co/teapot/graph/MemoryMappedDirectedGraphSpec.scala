package co.teapot.graph

import java.io.{BufferedWriter, File, FileWriter, IOException}
import java.nio.file.NoSuchFileException

import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.ArrayBuffer

class MemoryMappedDirectedGraphSpec extends WordSpec with Matchers {
  val testGraph1 = DirectedGraph(1 -> 2, 1 -> 3, 3 -> 1, 3 -> 2, 5 -> 1)

  def stringToTemporaryFile(contents: String): File = {
    val f = File.createTempFile("invalidGraph1", ".txt")
    val writer = new FileWriter(f)
    writer.write(contents)
    writer.close()
    f.deleteOnExit()
    f
  }

  def graphToEdgeFormat(graph: DirectedGraph,
                        edgeFile: File): Unit = {
    val edges = new ArrayBuffer[(Int, Int)]
    for (u <- graph.nodeIds) {
      for (v <- graph.outNeighbors(u)) {
        edges += ((u, v))
      }
    }
    edgesToFile(edges, edgeFile)
  }

  def edgesToFile(edges: Seq[(Int, Int)], edgeFile: File): Unit = {
    val writer = new BufferedWriter(new FileWriter(edgeFile))
    for ((u, v) <- edges) {
      writer.write(u + " " + v + "\n")
    }
    // Test that empty lines are accepted by parser
    writer.write("\n")
    writer.close()
  }

  "throw an error given an invalid filename" in {
    a[NoSuchFileException] should be thrownBy {
      MemoryMappedDirectedGraph(new File("nonexistant_file_4398219812437401"))
    }
  }
  for (segmentCount <- 1 to 3) {
    s" correctly read a list of edges for segmentCount $segmentCount" in {
      val tempBinaryFile = File.createTempFile("graph1", ".bin")
      val tempEdgeFile = File.createTempFile("graph1", ".txt")
      tempBinaryFile.delete() // Make sure edgeFileToGraph creates a file if it doesn't already exist
      graphToEdgeFormat(testGraph1, tempEdgeFile)

      new MemoryMappedDirectedGraphConverter(tempEdgeFile, tempBinaryFile,
        segmentCountOption = Some(segmentCount)).convert()

      val graph1 = new MemoryMappedDirectedGraph(tempBinaryFile)
      for (id <- testGraph1.nodeIds) {
        withClue(s"id: $id") {
          graph1.outNeighbors(id) should contain theSameElementsAs testGraph1.outNeighbors(id)
          graph1.inNeighbors(id) should contain theSameElementsAs testGraph1.inNeighbors(id)
        }
      }

      graph1.outNeighbor(1, 1) should equal (3)
      graph1.inNeighbor(1, 1) should equal (5)

      an[IndexOutOfBoundsException] should be thrownBy {
        graph1.outNeighbor(1, 2)
      }
      an[IndexOutOfBoundsException] should be thrownBy {
        graph1.outNeighbor(1, -1)
      }

      an[IndexOutOfBoundsException] should be thrownBy {
        graph1.outNeighbors(1)(2)
      }
      an[IndexOutOfBoundsException] should be thrownBy {
        graph1.outNeighbors(1)(-1)
      }

      an[NoSuchElementException] should be thrownBy {
        graph1.outNeighbor(6, 0)
      }
      an[NoSuchElementException] should be thrownBy {
        graph1.outNeighbors(6)
      }
      an[NoSuchElementException] should be thrownBy {
        graph1.outDegree(6)
      }

      graph1.existsNode(-1) shouldEqual false
      graph1.existsNode(6) shouldEqual false
      graph1.existsNode(1 << 29) shouldEqual false

      tempBinaryFile.delete()
      tempEdgeFile.delete()
    }

    s" correctly read edge lists and remove duplicates for segmentCount $segmentCount" in {
      val tempBinaryFile = File.createTempFile("graph1", ".bin")
      val edgeString = "1 \t 2\n\n\n1 0\n1 2\n3 \t 1\n1 5\n1 3\n"
      val tempEdgeFile = stringToTemporaryFile(edgeString)
      for (nodesPerChunk <- Seq(3, 5, Integer.MAX_VALUE)) {
        MemoryMappedDirectedGraphConverter.convert(tempEdgeFile, tempBinaryFile,
          segmentCountOption = Some(segmentCount))

        val graph = MemoryMappedDirectedGraph(tempBinaryFile)
        graph.nodeCount shouldEqual (6)
        graph.edgeCount shouldEqual(5)

        graph.outNeighbors(1) should contain theSameElementsInOrderAs Seq(0, 2, 3, 5)
        graph.inNeighbors(1) should contain theSameElementsInOrderAs Seq(3)
        graph.inNeighbors(2) should contain theSameElementsInOrderAs Seq(1)
        graph.inNeighbors(5) should contain theSameElementsInOrderAs Seq(1)
      }
      tempBinaryFile.delete()
      tempEdgeFile.delete()
    }
  }

  " throw an error given an invalid edge file" in {
    val outputFile = File.createTempFile("graph", "dat")
    val invalidFile1 = stringToTemporaryFile("1 2\n3")
    an[IOException] should be thrownBy {
      MemoryMappedDirectedGraphConverter.convert(invalidFile1, outputFile)
    }
    val invalidFile2 = stringToTemporaryFile("1 2\n3 4 5")
    an[IOException] should be thrownBy {
      MemoryMappedDirectedGraphConverter.convert(invalidFile2, outputFile)
    }
    val validFile1 = stringToTemporaryFile("1 \t\t 2\n\n\n3\t4")
    // Shouldn't throw an exception
    MemoryMappedDirectedGraphConverter.convert(validFile1, outputFile)
    outputFile.delete()
  }
}
