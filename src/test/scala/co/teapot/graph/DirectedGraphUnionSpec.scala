package co.teapot.graph

import org.scalatest.{FlatSpec, Matchers}

class DirectedGraphUnionSpec extends FlatSpec with Matchers {


  "A DirectedGraphUnion " should " represent the union of graphs " in {
    val graph1 = DynamicDirectedGraph(Array((1, 2), (1, 11)))
    val graph2 = DirectedGraph(Array((1, 2), (1, 20), (3, 21)))
    val graph3 = DirectedGraph(Array((1, 30)))
    val union = new DirectedGraphUnion(Seq(graph1, graph2, graph3))

    union.outNeighbors(1) should contain theSameElementsAs (Seq(2, 11, 2, 20, 30))
    union.inNeighbors(1) should contain theSameElementsAs (Seq())
    union.inNeighbors(2) should contain theSameElementsAs (Seq(1, 1))
    union.outNeighbors(3) should contain theSameElementsAs (Seq(21))

    // Check non-existent node
    union.existsNode(10) should be (false)

    // Make sure mutations in the underlying graphs are represented
    graph1.addEdge(3, 10)
    union.outNeighbors(3) should contain theSameElementsAs (Seq(10, 21))
    union.inNeighbors(10) should contain theSameElementsAs (Seq(3))

    // Make sure it works for a single graph
    val unionSingle = new DirectedGraphUnion(Seq(graph1))
    unionSingle.outNeighbors(1) should contain theSameElementsAs (Seq(2, 11))
  }
}
