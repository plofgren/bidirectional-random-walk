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

  "A MemoryMappedDirectedGraph" should {
    "correctly store and read a graph from binary" in {
      val tempFile = File.createTempFile("graph1", ".bin")
      MemoryMappedDirectedGraph.graphToFile(testGraph1, tempFile)
      val graph1 = new MemoryMappedDirectedGraph(tempFile)
      for (id <- testGraph1.nodeIds) {
        withClue(s"id $id") {
          testGraph1.outNeighbors(id) should contain theSameElementsAs (graph1.outNeighbors(id))
          testGraph1.inNeighbors(id) should contain theSameElementsAs (graph1.inNeighbors(id))
        }
      }
      graph1.getNodeById(-1) should be(None)
      graph1.getNodeById(6) should be(None)
      graph1.getNodeById(1 << 29) should be(None)
      tempFile.delete()

    }

    "throw an error given an invalid filename" in {
      a[NoSuchFileException] should be thrownBy {
        new MemoryMappedDirectedGraph(new File("nonexistant_file_4398219812437401"))
      }
    }

    " correctly read a list of edges" in {
      val tempBinaryFile = File.createTempFile("graph1", ".bin")
      val tempEdgeFile = File.createTempFile("graph1", ".txt")
      tempBinaryFile.delete() // Make sure edgeFileToGraph creates a file if it doesn't already exist
      graphToEdgeFormat(testGraph1, tempEdgeFile)
      MemoryMappedDirectedGraph.edgeFileToGraph(tempEdgeFile, tempBinaryFile, nodesPerChunk = 3)
      val graph1 = new MemoryMappedDirectedGraph(tempBinaryFile)
      for (id <- testGraph1.nodeIds) {
        graph1.outNeighbors(id) should contain theSameElementsAs testGraph1.outNeighbors(id)
        graph1.inNeighbors(id) should contain theSameElementsAs testGraph1.inNeighbors(id)
      }
      tempBinaryFile.delete()
      tempEdgeFile.delete()
    }

    " correctly read edge lists and remove duplicates" in {
      val tempBinaryFile = File.createTempFile("graph1", ".bin")
      val edgeString = "1 \t 2\n\n\n1 0\n1 2\n3 \t 1\n1 5\n1 3\n"
      val tempEdgeFile = stringToTemporaryFile(edgeString)
      for (nodesPerChunk <- Seq(3, 5, Integer.MAX_VALUE)) {
        MemoryMappedDirectedGraph.edgeFileToGraph(tempEdgeFile, tempBinaryFile, nodesPerChunk)
        val graph = new MemoryMappedDirectedGraph(tempBinaryFile)
        graph.nodeCount shouldEqual (6)
        graph.edgeCount shouldEqual(5)
        val node1 = graph.getNodeById(1).get
        node1.outboundNodes should contain theSameElementsInOrderAs Seq(0, 2, 3, 5)
        node1.inboundNodes should contain theSameElementsInOrderAs Seq(3)
        val node2 = graph.getNodeById(2).get
        node2.inboundNodes should contain theSameElementsInOrderAs Seq(1)
        val node5 = graph.getNodeById(5).get
        node5.inboundNodes should contain theSameElementsInOrderAs Seq(1)
      }
      tempBinaryFile.delete()
      tempEdgeFile.delete()
    }

    " throw an error given an invalid edge file" in {
      val outputFile = File.createTempFile("graph", "dat")
      val invalidFile1 = stringToTemporaryFile("1 2\n3")
      an[IOException] should be thrownBy {
        MemoryMappedDirectedGraph.edgeFileToGraph(invalidFile1, outputFile)
      }
      val invalidFile2 = stringToTemporaryFile("1 2\n3 4 5")
      an[IOException] should be thrownBy {
        MemoryMappedDirectedGraph.edgeFileToGraph(invalidFile1, outputFile)
      }
      val validFile1 = stringToTemporaryFile("1 \t\t 2\n\n\n3\t4")
      // Shouldn't throw an exception
      MemoryMappedDirectedGraph.edgeFileToGraph(validFile1, outputFile)
      outputFile.delete()
    }
  }
}
