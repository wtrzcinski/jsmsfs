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

package org.wtrzcinski.files.memory.ref

import org.wtrzcinski.files.memory.bitmap.BitmapEntry
import org.wtrzcinski.files.memory.exception.MemoryIllegalArgumentException

interface Block : BlockStart, BlockSize {

    companion object {
        fun of(byteOffset: Long, byteSize: Long): DefaultBlock {
            return DefaultBlock(start = byteOffset, size = byteSize)
        }

        fun of(byteOffset: Long, byteSize: Int): DefaultBlock {
            return DefaultBlock(start = byteOffset, size = byteSize.toLong())
        }
    }

    val middle: Long get() = (start + end) / 2

    val end: Long get() = start + size

    fun contains(other: BlockStart): Boolean {
        return this.start <= other.start && other.start <= this.end
    }

    fun contains(other: Block): Boolean {
        return this.start <= other.start && other.end <= this.end
    }

    fun subtract(other: Block): DefaultBlock {
        if (this.end == other.end) {
            return BitmapEntry(
                start = start,
                size = size - other.size,
            )
        }
        throw MemoryIllegalArgumentException()
    }

    fun divide(newSize: Long): Pair<Block, Block> {
        if (this.size > newSize) {
            val first = DefaultBlock(
                start = this.start,
                size = newSize,
            )
            val second = DefaultBlock(
                start = this.start + newSize,
                size = this.size - newSize
            )
            return first to second
        }
        throw MemoryIllegalArgumentException()
    }

    fun join(next: Block): DefaultBlock {
        if (this.end == next.start) {
            return DefaultBlock(
                start = this.start,
                size = this.size + next.size,
            )
        }
        throw MemoryIllegalArgumentException()
    }
}