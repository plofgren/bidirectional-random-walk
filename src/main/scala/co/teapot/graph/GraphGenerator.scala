package co.teapot.graph

object GraphGenerator {
  def completeGraph(nodeCount: Int): DirectedGraph = {
    // For efficiency, a future version could represent the graph implicitly
    val edges = for (u <- 0 until nodeCount;
                     v <- 0 until nodeCount;
                     if u != v) yield (u, v)
    DirectedGraph(edges)
  }
}
