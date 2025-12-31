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
import org.wtrzcinski.files.memory.lock.MemoryFileLock.Companion.use
import org.wtrzcinski.files.memory.lock.MemoryLockRegistry
import org.wtrzcinski.files.memory.mode.AbstractCloseable
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.util.Check
import org.wtrzcinski.files.memory.util.Require
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class BitmapRegistryGroup(
    offset: Long,
    mode: Mode = Mode.readWrite(),
    override val totalByteSize: ByteSize,
    private val locks: MemoryLockRegistry,
) : BitmapRegistry, Block, AbstractCloseable(mode = mode) {

    override val free: BitmapFreeBlocks = BitmapFreeBlocks()

    override val reserved: BitmapReservedBlocks = BitmapReservedBlocks()

    override val start: Long = offset

    override val size: Long = totalByteSize.size

    init {
        free.add(BitmapEntry(start = offset, size = totalByteSize.size))
    }

    override fun allocate(name: String, minBlockSize: ByteSize, maxBlockSize: ByteSize, prev: BlockStart): BitmapEntry {
        Check.isTrue { minBlockSize <= maxBlockSize }
        Check.isTrue { isWritable() }

        val lock = locks.bitmapLock
        lock.use {
            var result = free.findBySize(minByteSize = minBlockSize, maxByteSize = maxBlockSize)
            free.remove(result)
            if (result.size > maxBlockSize.size) {
                val divide = result.div(maxBlockSize)
                free.add(divide.second)
                result = divide.first
            }
            reserved.add(result)

            Check.isTrue {
                val sum = this.reserved.size + this.free.size
                totalByteSize == sum
            }
            return result
        }
    }

    override fun release(block: Block) {
        Check.isTrue { isWritable() }

        val lock = locks.bitmapLock
        lock.use {
            val segment = reserved.byEndOffset[block.end]
            if (segment != null) {
                val subtract = segment.minus(block)

                this.reserved.remove(segment)
                this.free.add(block)

                if (!subtract.isEmpty()) {
                    this.reserved.add(subtract)
                }
            } else {
                Require.unsupported()
            }

            Check.isTrue {
                val sum = this.reserved.size + this.free.size
                totalByteSize == sum
            }
        }
    }
}