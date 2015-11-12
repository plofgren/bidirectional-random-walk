/*
Copyright 2014 Stanford Social Algorithms Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
  limitations under the License.
*/

package soal.util

import org.scalatest.FlatSpec

class MappedPriorityQueueSpec extends FlatSpec {
  "A MappedPriorityQueueSpec" should "store priorities" in {
    val q = new HeapMappedIntPriorityQueue()
    q.insert(1, 1.0f)
    assert(q.getPriority(1) == 1.0f)
  }

  it should "store give default priority 0.0" in {
    val q = new HeapMappedIntPriorityQueue()
    q.insert(1, 1.0f)
    assert(q.getPriority(12345) == 0.0f)
  }

  it should "sort items" in {
    val q = new HeapMappedIntPriorityQueue()
    q.insert(2, 2.0f)
    q.insert(1, 1.0f)
    q.insert(-5, -5.0f)
    q.insert(4, 4.0f)
    q.increasePriority(3, 3.0f)
    assert(q.maxPriority == 4.0f)
    assert(q.extractMax() == 4)
    assert(q.extractMax() == 3)
    assert(q.extractMax() == 2)
    assert(q.extractMax() == 1)
    assert(q.extractMax() == -5)
    assert(q.isEmpty)
    //intecept[NoSuchElementException] {
    //  q.extractMax()
    //}
  }
  it should "sort items respecting  increased priorities" in {
    val q = new HeapMappedIntPriorityQueue()
    q.insert(2, 0.5f)
    q.insert(1, 1.0f)
    q.insert(4, 0.04f)
    q.increasePriority(3, 0.3f) // Should implicitly add
    assert(q.maxPriority == 1.0f)
    q.increasePriority(2, 2.0f)
    q.increasePriority(4, 4.0f)
    assert(q.currentPriorities() == Map(2-> 2.0f, 1 -> 1.0f, 3 -> 0.3f, 4 -> 4.0f), q.currentPriorities())
    q.increasePriority(3, 3.0f)
    assert(q.getPriority(3) == 3.0f)
    assert(q.maxPriority == 4.0f)
    assert(q.extractMax() == 4)
    assert(q.extractMax() == 3)
    assert(q.extractMax() == 2)
    assert(q.extractMax() == 1)
    assert(q.isEmpty)
    //intecept[NoSuchElementException] {
    //  q.extractMax()
    //}
  }
}
