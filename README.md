This repository contains efficient personalized PageRank algorithms and a library for large 
graphs.

# Bidirectional Random Walk Estimation
This algorithm is described in the paper Personalized PageRank Estimation and Search: A 
Bidirectional Approach by Peter Lofgren, Sid Banerjee, and Ashish Goel and more fully in Peter's 
PhD thesis.

As an example, we'll compute PPR scores between nodes 1 and 2 using teleport probability 0.2 on the 
small graph in 
/src/test/resources/test_graph.txt.  First install java, sbt, and git.  Then simply run

```
git checkout https://github.com/plofgren/bidirectional-random-walk.git
cd bidirectional-random-walk
sbt console
val graph = co.teapot.graph.ConcurrentHashMapDynamicGraph.readGraph("src/test/resources/test_graph
.txt")
val estimator = new soal.ppr.BidirectionalPPREstimator(graph, teleportProbability=0.2f)
estimator.estimatePPRSingleSource(0, 1)
estimator.estimatePPRSingleSource(0, 1, relativeError=0.01f) // For greater accuracy
```

Our algorithm can also start from a distribution over source nodes rather than a single node. For
 example, if we want to estimate PPR scores using random walks that start at node 0 with 
 probability 0.8 and node 2 with probability 0.2, we can run
 
```
val sourceDistribution = new soal.util.DiscreteAliasSampler(Seq(0, 2), Seq(0.8f, 0.2f))
estimator.estimatePPR(sourceDistribution, 1)
```

# Graph Library
To handle large graphs efficiently, this repository provides a MemoryMappedDirectedGraph class,
which stores graphs in an efficient binary format that requires only 8 bytes per node and 8 bytes
 per edge.  The data is stored in a memory mapped file, which has two benefits relative to 
 storing it in arrays:
 1. There is no object overehead per node and no garbage collector overhead.
 2. If the graph file is in the OS cache, the graph loads almost instantly.  Rather than
 parse a text file, the load operation is a set of memory map calls.

For large graphs, you will want to first convert your graph to a MemoryMappedDirectedGraph.  The 
input to our converter is a simple text format where each line has the form "id1 id2" to 
indicate an edge from id1 to id2, where id1 and id2 are Ints.  To convert the test graph, for 
example, run:
```
sbt assembly
java -Xmx2g -cp target/scala-2.11/bidirectional-random-walk-assembly-1.0.jar\
  co.teapot.graph.MemoryMappedDirectedGraphConverter \
  src/test/resources/test_graph.txt test_graph.dat
```
 
Then in Scala you can run graph operations.  For example:
```
sbt console
val graph = co.teapot.graph.MemoryMappedDirectedGraph("test_graph.dat")
graph.nodeCount
graph.edgeCount
graph.outDegree(4)
graph.outNeighbors(4)
graph.inNeighbors(4)
```
