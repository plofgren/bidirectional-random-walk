/*
 * Copyright 2016 Teapot, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package co.teapot.graph

trait DynamicDirectedGraph extends DirectedGraph {
  /** Adds the given edge to the graph, increasing the number of nodes if needed.
    */
  def addEdge(id1: Int, id2: Int): Unit
}

object DynamicDirectedGraph {
  def apply(edges: (Int, Int)*): DynamicDirectedGraph =
    DynamicDirectedGraph(edges)

  def apply(edges: Iterable[(Int, Int)]): DynamicDirectedGraph =
    ConcurrentHashMapDynamicGraph(edges)
}
