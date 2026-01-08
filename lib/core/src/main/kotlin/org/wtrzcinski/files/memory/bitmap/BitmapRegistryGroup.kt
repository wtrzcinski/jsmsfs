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

package org.wtrzcinski.files.memory.bitmap

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.lock.MemoryFileLock.Companion.use
import org.wtrzcinski.files.memory.lock.MemoryLockRegistry
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.ModeMonitor
import org.wtrzcinski.files.memory.util.Check
import org.wtrzcinski.files.memory.util.Require
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class BitmapRegistryGroup(
    offset: Long,
    mode: Mode = Mode.create(),
    override val totalByteSize: ByteSize,
    private val locks: MemoryLockRegistry,
    private val compact: Boolean,
) : BitmapRegistry, Block, ModeMonitor(mode = mode) {

    override val free: BitmapFreeBlocks = BitmapFreeBlocks(compact)

    override val reserved: BitmapReservedBlocks = BitmapReservedBlocks()

    override val start: Long = offset

    override val size: Long = totalByteSize.size

    init {
        free.add(BitmapEntry(start = offset, size = totalByteSize.size))
    }

    override fun isReadOnly(): Boolean {
        return isSafe()
    }

    override fun isReserved(ref: BlockAddress): Boolean {
        val entry = reserved.byStartOffset[ref.start]
        return entry != null
    }

    override fun allocate(range: ClosedRange<ByteSize>, exactBlockSize: ByteSize, prev: BitmapEntry?): BitmapEntry {
        Check.isTrue { isCreating() }

        val lock = locks.bitmapLock
        lock.use {
            var result = free.find(
                headerSize = range.start,
                maxByteSize = range.endInclusive,
                exactBlockSize = exactBlockSize,
                prev = prev
            )
            free.remove(result)

            if (result.size > exactBlockSize.size) {
                val divide = result.div(exactBlockSize)
                free.add(divide.second)
                result = divide.first
            }

            if (prev != null && prev.isValid()) {
                if (compact) {
                    if (prev.endExclusive == result.start) {
                        reserved.remove(prev)
                        result = prev + result
                    }
                }
            }
            reserved.add(result.withPrev(prev = prev?.start))

            Check.isTrue {
                val sum = this.reserved.size + this.free.size
                totalByteSize == sum
            }
            Check.isTrue {
                result.size == exactBlockSize.size
            }
            return result
        }
    }

    override fun allocate(range: ClosedRange<ByteSize>, prev: BitmapEntry?): BitmapEntry {
        Check.isTrue { isCreating() }

        val lock = locks.bitmapLock
        lock.use {
            var result = free.find(
                headerSize = range.start,
                maxByteSize = range.endInclusive,
                prev = prev
            )
            free.remove(result)

            if (result.size > range.endInclusive.size) {
                val divide = result.div(range.endInclusive)
                free.add(divide.second)
                result = divide.first
            }

            if (prev != null && prev.isValid()) {
                if (compact) {
                    if (prev.endExclusive == result.start) {
                        reserved.remove(prev)
                        result = prev + result
                    }
                }
            }
            reserved.add(result.withPrev(prev = prev?.start))

            Check.isTrue {
                val sum = this.reserved.size + this.free.size
                totalByteSize == sum
            }
            return result
        }
    }

    override fun release(block: Block) {
        Check.isTrue { isCreating() }

        val lock = locks.bitmapLock
        lock.use {
            val segment = reserved.byEndOffset[block.endExclusive]
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