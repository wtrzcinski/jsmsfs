/**
 * Copyright 2026 Wojciech Trzciński
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

package org.wtrzcinski.memory.address

import org.wtrzcinski.memory.bitmap.BitmapEntry
import org.wtrzcinski.memory.exception.MemoryIllegalArgumentException

interface Block : BlockAddress, BlockSize, OpenEndRange<Long> {

    companion object {

        fun of(byteOffset: Long, byteSize: Long): DefaultBlock {
            return DefaultBlock(start = byteOffset, size = byteSize)
        }

        fun of(byteOffset: Long, byteSize: Int): DefaultBlock {
            return DefaultBlock(start = byteOffset, size = byteSize.toLong())
        }
    }

    override val start: Long

    override val endExclusive: Long get() = start + size

    val middle: Long get() = (start + endExclusive) / 2

    override fun isEmpty(): Boolean {
        return size == 0L
    }

    operator fun contains(other: BlockAddress): Boolean {
        return this.start <= other.start && other.start <= this.endExclusive
    }

    operator fun contains(other: Block): Boolean {
        return this.start <= other.start && other.endExclusive <= this.endExclusive
    }

    operator fun minus(other: Block): DefaultBlock {
        val thisEnd = this.endExclusive
        val otherEnd = other.endExclusive
        if (thisEnd == otherEnd) {
            return BitmapEntry(start = start, size = size - other.size, first = this)
        }
        throw MemoryIllegalArgumentException()
    }

    operator fun div(newSize: DefaultBlockSize): Pair<Block, Block> {
        if (this.size > newSize.size) {
            val first = DefaultBlock(
                start = this.start,
                size = newSize.size,
            )
            val second = DefaultBlock(
                start = this.start + newSize.size,
                size = this.size - newSize.size,
            )
            return first to second
        }
        throw MemoryIllegalArgumentException()
    }

    operator fun plus(next: Block): DefaultBlock {
        if (this.endExclusive == next.start) {
            return DefaultBlock(start = this.start, size = this.size + next.size)
        }
        throw MemoryIllegalArgumentException()
    }

}