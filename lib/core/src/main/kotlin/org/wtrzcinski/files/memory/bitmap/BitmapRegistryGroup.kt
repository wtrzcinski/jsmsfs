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
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryOpenOptions.Companion.WRITE
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.lock.MemoryFileLock.Companion.use
import org.wtrzcinski.files.memory.lock.MemoryLockRegistry
import org.wtrzcinski.files.memory.lock.ReadWriteMemoryFileLock
import org.wtrzcinski.files.memory.util.Preconditions.assertTrue
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class BitmapRegistryGroup(
    memoryOffset: Long,
    val totalByteSize: ByteSize,
    private val readOnly: Boolean,
    private val lockRegistry: MemoryLockRegistry,
) : BitmapRegistry {

    override val free: BitmapFreeBlocks = BitmapFreeBlocks()

    override val reserved: BitmapReservedBlocks = BitmapReservedBlocks()

    init {
        free.add(BitmapEntry(start = memoryOffset, size = totalByteSize.size))
    }

    override fun isReadOnly(): Boolean {
        return readOnly
    }

    override fun allocate(
        minBlockSize: ByteSize,
        maxBlockSize: ByteSize,
        prev: BlockStart,
    ): BitmapEntry {
        assertTrue(!isReadOnly())
        assertTrue(minBlockSize <= maxBlockSize)

        val lock = lockRegistry.newLock(mode = WRITE)

        lock.use {
            var result = free.findBySize(minByteSize = minBlockSize, maxByteSize = maxBlockSize)
            free.remove(result)
            if (result.size > maxBlockSize.size) {
                val divide = result.div(maxBlockSize)
                free.add(divide.second)
                result = divide.first
            }
            val withPrev = BitmapEntry(start = result.start, size = result.size, prev = prev)
            reserved.add(withPrev)

            val sum = reserved.byteSize + free.size
            require(totalByteSize == sum) { "$totalByteSize != $sum" }

            return withPrev
        }
    }

    override fun release(block: Block) {
        assertTrue(!isReadOnly())

        val lock = lockRegistry.newLock(mode = WRITE)

        lock.use {
            val reserved = reserved.copy()

            val addToFree = mutableListOf<Block>()
            val addToReserved = mutableListOf<BitmapEntry>()
            val removeFromReserved = mutableListOf<BitmapEntry>()
            for (segment in reserved) {
                if (segment.start == block.start) {
                    require(segment.size == block.size)

                    removeFromReserved.add(segment)
                    addToFree.add(segment)
                } else if (segment.contains(block)) {
                    val subtract = segment.minus(block)

                    removeFromReserved.add(segment)
                    addToReserved.add(subtract)
                    addToFree.add(block)
                }
            }
            for (segment in reserved) {
                if (addToFree.any { it.start == segment.prev.start }) {
                    removeFromReserved.add(segment)
                    addToFree.add(segment)
                }
            }

            for (it in removeFromReserved) {
                this.reserved.remove(it)
            }

            for (it in addToReserved) {
                this.reserved.add(it)
            }

            for (it in addToFree) {
                free.add(it)
            }

            require(totalByteSize == this.reserved.byteSize + this.free.size)
        }
    }
}