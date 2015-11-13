package soal.util

import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.graph.{DirectedGraph, Node, StoredGraphDir}
import com.twitter.cassovary.util.NodeNumberer
import com.twitter.cassovary.util.io.AdjacencyListGraphReader

object CassovaryUtil {
  def readGraph(graphPath: String): DirectedGraph[Node] = {
    val filenameStart = graphPath.lastIndexOf('/') + 1
    val graphDirectory = graphPath.take(filenameStart)
    val graphFilename = graphPath.drop(filenameStart)
    val reader = new AdjacencyListGraphReader(
      graphDirectory,
      graphFilename,
      new NodeNumberer.IntIdentity(),
      _.toInt) {
      override def storedGraphDir: StoredGraphDir = StoredGraphDir.BothInOut
    }
    reader.toArrayBasedDirectedGraph(): DirectedGraph[Node]
  }
}
