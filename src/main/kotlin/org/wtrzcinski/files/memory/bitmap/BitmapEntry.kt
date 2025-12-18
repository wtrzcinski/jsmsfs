/**
 * Copyright 2025 Wojciech Trzciński
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wtrzcinski.files.memory.bitmap

import org.wtrzcinski.files.memory.byteBuffer.MemoryBlockByteBuffer
import org.wtrzcinski.files.memory.ref.Block
import org.wtrzcinski.files.memory.ref.DefaultBlock

class BitmapEntry(
    start: Long,
    size: Long,
    end: Long = start + size,
    val prev: Long = MemoryBlockByteBuffer.invalidRef,
    val name: String? = null,
) : DefaultBlock(start = start, size = size, end = end) {

    override fun subtract(other: Block): BitmapEntry {
        val subtract = super.subtract(other)
        return BitmapEntry(
            start = subtract.start,
            size = subtract.size,
            prev = this.prev,
            name = this.name,
        )
    }

    override fun divide(newSize: Long): Pair<BitmapEntry, BitmapEntry> {
        val divide = super.divide(newSize)
        val first = BitmapEntry(
            start = divide.first.start,
            size = divide.first.size,
            prev = this.prev,
            name = this.name,
        )
        val second = BitmapEntry(
            start = divide.second.start,
            size = divide.second.size,
            prev = divide.first.start,
        )
        return first to second
    }

    override fun join(next: Block): BitmapEntry {
        val join = super.join(next)
        return BitmapEntry(
            start = join.start,
            size = join.size,
            prev = this.prev,
            name = this.name,
        )
    }

    fun isFirst(): Boolean {
        return prev == MemoryBlockByteBuffer.invalidRef
    }

    override fun toString(): String {
        return "${javaClass.simpleName}(start=$start, end=$end, size=$size, prev=$prev, name=$name)"
    }
}