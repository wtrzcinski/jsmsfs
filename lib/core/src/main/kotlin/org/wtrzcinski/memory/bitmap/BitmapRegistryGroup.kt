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
import org.wtrzcinski.memory.address.DefaultBlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.lock.MemoryFileLock.Companion.use
import org.wtrzcinski.memory.lock.MemoryLockRegistry
import org.wtrzcinski.memory.mode.Mode
import org.wtrzcinski.memory.mode.ModeMonitor
import org.wtrzcinski.memory.util.Check
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class BitmapRegistryGroup(
    ref: Long,
    mode: Mode = Mode.create(),
    override val totalByteSize: DefaultBlockSize,
    private val locks: MemoryLockRegistry,
    private val compact: Boolean,
) : BitmapRegistry, Block, ModeMonitor(mode = mode) {

    override val free: BitmapFreeBlocks = BitmapFreeBlocks(compact)

    override val reserved: BitmapReservedBlocks = BitmapReservedBlocks()

    override val start: Long = ref

    override val size: Long = totalByteSize.size

    init {
        free.add(BitmapEntry(start = ref, size = totalByteSize.size, first = DefaultBlockAddress(ref)))
    }

    override fun isReadOnly(): Boolean {
        return isSafe()
    }

    override fun isReserved(ref: BlockAddress): Boolean {
        val entry = reserved.byStartOffset[ref.start]
        return entry != null
    }

    override fun allocate(range: ClosedRange<DefaultBlockSize>, exactBlockSize: DefaultBlockSize): BitmapEntry {
        Check.isTrue { isCreating() }

        val lock = locks.bitmapLock
        lock.use {
            var result = free.find(
                minByteSize = range.start,
                maxByteSize = range.endInclusive,
                exactBlockSize = exactBlockSize,
            )
            free.remove(result)

            if (result.size > exactBlockSize.size) {
                val divide = result.div(exactBlockSize)
                free.add(divide.second)
                result = divide.first
            }

            reserved.add(result)

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

    override fun allocate(range: ClosedRange<DefaultBlockSize>, first: BlockAddress?, prev: BitmapEntry?): BitmapEntry {
        Check.isTrue { isCreating() }

        val lock = locks.bitmapLock
        lock.use {
            var result = free.find(
                minByteSize = range.start,
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
                if (this.compact) {
                    if (prev.endExclusive == result.start) {
                        this.reserved.remove(prev)
                        result = prev + result
                    }
                }
                result = result.withPrev(prev = prev)
            }
            if (first != null && first.isValid()) {
                result = result.withFirst(first)
            }
            reserved.add(result)

            Check.isTrue {
                val sum = this.reserved.size + this.free.size
                totalByteSize == sum
            }
            return result
        }
    }

    override fun release(block: Block): ReleaseResult {
        Check.isTrue { isCreating() }

        val lock = locks.bitmapLock
        lock.use {
            val segment = reserved.byEndOffset[block.endExclusive]
            requireNotNull(segment)
            val subtract = segment.minus(block)

            this.reserved.remove(segment)
            this.free.add(block)

            if (!subtract.isEmpty()) {
                this.reserved.add(subtract)
            }

            Check.isTrue {
                val sum = this.reserved.size + this.free.size
                totalByteSize == sum
            }

            val prev: BitmapEntry? = reserved.byEndOffset[segment.start]
            val next: BitmapEntry? = reserved.byStartOffset[segment.endExclusive]
            if (prev != null && next != null) {
                if (prev == next.prev) {
                    return ReleaseResult(neighbors = listOf(prev to next))
                }
            }
            return ReleaseResult()
        }
    }
}