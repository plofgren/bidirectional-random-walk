# Bidirectional Random Walk
This repository contains a bidirectional random walk (Personalized PageRank) estimation algorithm
for large graphs. This algorithm is described in the paper[Personalized PageRank Estimation and 
Search: A Bidirectional Approach](http://arxiv.org/abs/1507.05999) and more fully in [Peter's PhD thesis](http://arxiv.org/abs/1512.04633).

As an example, we'll compute PPR scores between nodes 1 and 2 using teleport probability 0.2 on the
small graph in 
/src/test/resources/test_graph.txt.  First install java, sbt, and git.  Then simply run

```
git clone https://github.com/plofgren/bidirectional-random-walk.git
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

To support large graphs, this library uses the [Tempest Graph Library](https://github.com/teapot-co/tempest) which provides an efficient graph 
implementation based on memory mapped files.
