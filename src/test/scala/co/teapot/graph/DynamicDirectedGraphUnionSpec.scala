package co.teapot.graph

import org.scalatest.{Matchers, WordSpec}

class DynamicDirectedGraphUnionSpec extends WordSpec with Matchers {
  val staticGraph1 = DirectedGraph(1 -> 2, 1 -> 3, 3 -> 1, 3 -> 2, 5 -> 1)

  "A DynamicDirectedGraph" should {
    " correctly combine two graphs" in {
      staticGraph1.nodeCount shouldEqual (4)
      staticGraph1.edgeCount shouldEqual(5)
      val dynamicGraph = new ConcurrentHashMapDynamicGraph()
      val unionGraph = new DynamicDirectedGraphUnion(staticGraph1, dynamicGraph)
      unionGraph.maxNodeId shouldEqual (5)

      unionGraph.addEdge(5, 6)
      unionGraph.nodeIds should contain theSameElementsAs (Seq(1, 2, 3, 5, 6))
      unionGraph.nodeCount shouldEqual (5)
      unionGraph.edgeCount shouldEqual (6)
      unionGraph.maxNodeId shouldEqual (6)
      unionGraph.outNeighbors(5) should contain theSameElementsAs (Seq(1, 6))
      unionGraph.inNeighbors(6) should contain theSameElementsAs (Seq(5))
      unionGraph.outNeighbors(5) should contain theSameElementsAs (Seq(1, 6))
      // Make sure getNodeById doesn't create the node
      unionGraph.existsNode(4) should be (false)
      unionGraph.nodeCount shouldEqual (5)
      unionGraph.addEdge(1, 4)
      unionGraph.nodeCount shouldEqual (6)
      unionGraph.inNeighbors(4) should contain theSameElementsAs (Seq(1))
      unionGraph.addEdge(1, 6)
      unionGraph.outNeighbors(1) should contain theSameElementsAs (Seq(2, 3, 4, 6))
      unionGraph.edgeCount shouldEqual (8)
    }

    " throw an exception given a null graph" in {
      a[NullPointerException] should be thrownBy {
        new DynamicDirectedGraphUnion(staticGraph1, null)
      }
    }
  }
}
