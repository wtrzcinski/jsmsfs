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

import org.wtrzcinski.files.memory.ref.Block
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.lock.MemoryFileLock.Companion.use
import org.wtrzcinski.files.memory.lock.ReadWriteMemoryFileLock

class BitmapRegistryGroup(
    memoryOffset: Long,
    val totalByteSize: Long,
    private val readOnly: Boolean,
) : BitmapRegistry {

    private val lock: MemoryFileLock = ReadWriteMemoryFileLock()

    override val free: BitmapFreeBlocks = BitmapFreeBlocks()

    override val reserved: BitmapReservedBlocks = BitmapReservedBlocks()

    init {
        free.add(BitmapEntry(start = memoryOffset, size = totalByteSize))
    }

    override fun isReadOnly(): Boolean {
        return readOnly
    }

    override fun reserveBySize(byteSize: Long, prev: Long, tag: String?): BitmapEntry {
        lock.use {
            var result = free.findBySize(byteSize)
            free.remove(result)
            if (result.size > byteSize) {
                val divide = result.divide(byteSize)
                free.add(divide.second)
                result = divide.first
            }
            val withPrev = BitmapEntry(start = result.start, size = result.size, prev = prev, name = tag)
            reserved.add(withPrev)

            require(totalByteSize == reserved.size + free.size)

            return withPrev
        }
    }

    override fun releaseAll(other: Block) {
        lock.use {
            val reserved = reserved.copy()

            val addToFree = mutableListOf<Block>()
            val addToReserved = mutableListOf<BitmapEntry>()
            val removeFromReserved = mutableListOf<BitmapEntry>()
            for (segment in reserved) {
                if (segment.start == other.start) {
                    require(segment.size == other.size)

                    removeFromReserved.add(segment)
                    addToFree.add(segment)
                } else if (segment.contains(other)) {
                    val subtract = segment.subtract(other)

                    removeFromReserved.add(segment)
                    addToReserved.add(subtract)
                    addToFree.add(other)
                }
            }
            for (segment in reserved) {
                if (addToFree.any { it.start == segment.prev }) {
                    removeFromReserved.add(segment)
                    addToFree.add(segment)
                }
            }

            for (it in addToReserved) {
                this.reserved.add(it)
            }

            for (it in removeFromReserved) {
                this.reserved.remove(it)
            }

            for (it in addToFree) {
                free.add(it)
            }

            require(totalByteSize == this.reserved.size + this.free.size)
        }
    }
}