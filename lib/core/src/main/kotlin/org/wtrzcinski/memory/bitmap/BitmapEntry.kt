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

package org.wtrzcinski.memory.bitmap

import org.wtrzcinski.memory.address.Block
import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlock
import org.wtrzcinski.memory.address.DefaultBlockSize

class BitmapEntry(
    start: Long,
    size: Long,
    val first: BlockAddress,
    val prev: BlockAddress? = null,
) : DefaultBlock(
    start = start,
    size = size,
    endExclusive = start + size
) {

    constructor(
        block: Block,
        first: BlockAddress = block,
        prev: BlockAddress? = null,
    ) : this(
        start = block.start,
        size = block.size,
        first = first,
        prev = prev,
    )

    override operator fun minus(other: Block): BitmapEntry {
        val subtract = super.minus(other)
        return BitmapEntry(block = subtract, first = this.first, prev = this.prev)
    }

    override operator fun div(newSize: DefaultBlockSize): Pair<BitmapEntry, BitmapEntry> {
        val divide = super.div(newSize)
        val first = BitmapEntry(block = divide.first, first = this.first, prev = this.prev)
        val second = BitmapEntry(block = divide.second, first = divide.second)
        return first to second
    }

    override operator fun plus(next: Block): BitmapEntry {
        val join = super.plus(next)
        return BitmapEntry(block = join, first = this.first, prev = this.prev)
    }

    fun withFirst(first: BlockAddress): BitmapEntry {
        return BitmapEntry(block = this, first = first, prev = this.prev)
    }

    fun withPrev(prev: BlockAddress?): BitmapEntry {
        return BitmapEntry(block = this, first = this.first, prev = prev)
    }

    override fun toString(): String {
        return "${javaClass.simpleName}(start=$start, end=$endExclusive, size=$size, first=$first, prev=$prev)"
    }
}