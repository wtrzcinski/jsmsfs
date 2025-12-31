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

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.address.DefaultBlock

class BitmapEntry(
    start: Long,
    size: Long,
) : DefaultBlock(
    start = start,
    size = size,
    end = start + size
) {

    constructor(block: Block): this(start = block.start, size = block.size)

    override operator fun minus(other: Block): BitmapEntry {
        val subtract = super.minus(other)
        return BitmapEntry(
            start = subtract.start,
            size = subtract.size,
        )
    }

    override operator fun div(newSize: ByteSize): Pair<BitmapEntry, BitmapEntry> {
        val divide = super.div(newSize)
        val first = BitmapEntry(
            start = divide.first.start,
            size = divide.first.size,
        )
        val second = BitmapEntry(
            start = divide.second.start,
            size = divide.second.size,
        )
        return first to second
    }

    override operator fun plus(next: Block): BitmapEntry {
        val join = super.plus(next)
        return BitmapEntry(
            start = join.start,
            size = join.size,
        )
    }

    override fun toString(): String {
        return "${javaClass.simpleName}(start=$start, end=$end, size=$size)"
    }
}