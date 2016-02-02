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
package co.teapot.io

import java.nio.MappedByteBuffer

import ByteBufferIntSlice.BytesPerInt

class ByteBufferIntSlice(val data: MappedByteBuffer,
                         val offset: Int,
                         val length: Int) extends IndexedSeq[Int] {
  def apply(i: Int) = data.getInt(offset + BytesPerInt * i)
}
object ByteBufferIntSlice {
  val BytesPerInt = 4
}