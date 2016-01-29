package co.teapot.graph

import org.scalatest.{Matchers, WordSpec}

class ConcurrentHashMapDynamicGraphSpec extends WordSpec with Matchers {
  "An Efficient SynchronizedDynamicDirectedGraphSpec" should {
    "support adding nodes" in {
      val graph = new ConcurrentHashMapDynamicGraph()
      for (i <- 1 to 3) {
        graph.edgeCount shouldEqual (i - 1)
        graph.addEdge(10 * i, 1) // non-contiguous
        graph.nodeCountOption.get shouldEqual (i + 1)
        graph.maxNodeId shouldEqual (10 * i)
      }
      graph.addEdge(10, 20) // Adding edges to existing nodes should not increase node count
      graph.nodeCountOption.get shouldEqual 4
      graph.existsNode(1000000) shouldEqual false

      graph.outNeighbors(10) should contain theSameElementsAs (Seq(1, 20))
      graph.inNeighbors(1) should contain theSameElementsAs (Seq(10, 20, 30))

      an[IndexOutOfBoundsException] should be thrownBy {
        graph.outNeighbor(10, 2)
      }
      a[NoSuchElementException] should be thrownBy {
        graph.outNeighbor(7, 0)
      }
      a[NoSuchElementException] should be thrownBy {
        graph.outDegree(7)
      }
    }

    "support adding edges" in {
      val graph = new ConcurrentHashMapDynamicGraph()
      graph.addEdge(1, 2)
      // For now, addEdge allows duplicates.  graph.addEdge(1, 2) // Test duplicate elimination
      graph.edgeCount shouldEqual 1
      graph.maxNodeId shouldEqual 2
      val node1 = graph.getNodeById(1).get
      node1.inboundNodes.toList shouldEqual ( List())
      node1.outboundNodes.toList shouldEqual (List(2))
      val node2 = graph.getNodeById(2).get
      node2.inboundNodes.toList shouldEqual (List(1))
      node2.outboundNodes.toList shouldEqual (List())

      // test immutability of outboundNodes
      val oldOutboundNodes1: Seq[Int] = node1.outboundNodes()
      val oldInboundNodes2: Seq[Int] = node2.inboundNodes()
      graph.addEdge(1, 10)
      graph.addEdge(200, 2)
      oldOutboundNodes1.toList shouldEqual (List(2))
      oldInboundNodes2.toList shouldEqual (List(1))

      graph.maxNodeId shouldEqual 200

      // Test multi-edge
      graph.addEdge(1, 2)
      node1.inboundNodes.toList shouldEqual (List())
      node1.outboundNodes.toList shouldEqual (List(2, 10, 2))
      node2.inboundNodes.toList shouldEqual (List(1, 200, 1))
      node2.outboundNodes.toList shouldEqual (List())
    }

    "support concurrent writing" in {
      val edgesPerThread = 10
      val threadCount = 8
      val graph = new ConcurrentHashMapDynamicGraph()
      val edgeAdders = (0 until threadCount) map { threadIndex =>
        new Thread() {
          override def run(): Unit = {
            for (i <- 0 until edgesPerThread) {
              graph.addEdge(0, threadIndex * edgesPerThread + i)
            }
          }
        }
      }
      val edgeReader = new Thread() {
        override def run(): Unit = {
          for (i <- 0 until 5) {
            val neighbors0 =
              if (graph.existsNode(0)) graph.outNeighbors(0) else IndexedSeq[Int]()
            Thread.sleep(1)
            assert(neighbors0.count(_ == 0) <= 1)  // I expect the most common error would be observing extra 0 entries
          }
        }
      }
      edgeReader.start()
      edgeAdders foreach (_.start())
      edgeAdders foreach (_.join())
      edgeReader.join()
      graph.outNeighbors(0) should contain theSameElementsAs (0 until edgesPerThread * threadCount)

    }
  }
}
