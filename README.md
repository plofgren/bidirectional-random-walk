# Bidirectional Random Walk
This repository contains a bidirectional random walk (Personalized PageRank) estimation algorithm 
for large 
graphs.  This algorithm is described in the paper Personalized PageRank Estimation and Search: A 
Bidirectional Approach by Peter Lofgren, Sid Banerjee, and Ashish Goel and more fully in Peter's 
PhD thesis.

As an example, we'll compute PPR scores between nodes 1 and 2 using teleport probability 0.2 on the 
small graph in 
/src/test/resources/test_graph.txt.  First install java, sbt, and git.  Then simply run

```
git checkout https://github.com/plofgren/bidirectional-random-walk.git
cd bidirectional-random-walk
sbt console
val graph = soal.util.CassovaryUtil.readGraph("src/test/resources/test_graph.txt")
val estimator = new soal.ppr.BidirectionalPPREstimator(graph, teleportProbability=0.2f)
estimator.estimatePPRSingleSource(0, 1)
estimator.estimatePPRSingleSource(0, 1, relativeError=0.01f) // For greater accuracy
```

For large graphs, you likely will want to first convert your graph to a [MemoryMappedDirectedGraph]
(https://github.com/twitter/cassovary/blob/master/cassovary-core/src/main/scala/com/twitter/cassovary/graph/MemoryMappedDirectedGraph.scala)
 to improve load time and RAM usage.
 