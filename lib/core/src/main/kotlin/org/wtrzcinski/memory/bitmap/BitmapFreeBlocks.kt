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
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.exception.OptimisticLockException
import org.wtrzcinski.memory.exception.OutOfMemoryException
import org.wtrzcinski.memory.util.Check
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.minusAssign
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
class BitmapFreeBlocks(
    val compact: Boolean,
) {

    interface FindStrategy {
        fun find(free: BitmapFreeBlocks): Block
    }

    private val byStartOffset: TreeMap<Long, BitmapEntry> = TreeMap()

    private val byEndOffset: MutableMap<Long, BitmapEntry> = ConcurrentHashMap()

    private val bySize: MutableMap<Long, CopyOnWriteArrayList<BitmapEntry>> = ConcurrentHashMap()

    private var freeSize: AtomicLong = AtomicLong(0L)

    val size: DefaultBlockSize
        get() {
            return DefaultBlockSize(freeSize.load())
        }

    fun findSizeSum(segmentSizeLt: DefaultBlockSize): Double {
        var result = 0.0
        for (entry in byStartOffset.values) {
            if (entry.size < segmentSizeLt.size) {
                result += entry.size
            }
        }
        return result
    }

    fun add(current: Block) {
        Check.isNull { findByStartOffset(current.start) }

        Check.isNull { findByStartOffset(current.middle) }

        val next = findByStartOffset(current.endExclusive)
        if (next != null) {
            remove(next)
            val join = current.plus(next)
            add(current = join)
        } else {
            val prev = findByEndOffset(current.start)
            if (prev != null) {
                remove(prev)
                val join = prev.plus(current)
                add(current = join)
            } else {
                doAdd(BitmapEntry(current))
            }
        }
    }

    private fun doAdd(other: BitmapEntry): BitmapFreeBlocks {
        this.freeSize += other.size
        this.byStartOffset[other.start] = other
        this.byEndOffset[other.endExclusive] = other
        val bySizeList = this.bySize.computeIfAbsent(other.size) { CopyOnWriteArrayList() }
        bySizeList.add(other)
        return this
    }

    fun remove(other: Block) {
        this.byStartOffset.remove(other.start) ?: throw OptimisticLockException()
        this.byEndOffset.remove(other.endExclusive) ?: throw OptimisticLockException()
        val bySizeList = this.bySize[other.size] ?: throw OptimisticLockException()
        bySizeList.remove(other)
        if (bySizeList.isEmpty()) {
            this.bySize.remove(other.size)
        }
        this.freeSize -= other.size
    }

    fun find(
        minByteSize: DefaultBlockSize,
        maxByteSize: DefaultBlockSize,
        exactBlockSize: DefaultBlockSize? = null,
        prev: BitmapEntry? = null,
    ): BitmapEntry {
        Check.isTrue { minByteSize.isValid() }
        Check.isTrue { maxByteSize.isValid() }
        Check.isTrue { minByteSize <= maxByteSize }

        if (prev != null && prev.isValid()) {
            val neighbourNext = byStartOffset[prev.endExclusive]
            if (neighbourNext != null) {
                if (neighbourNext.size >= minByteSize.size) {
                    return neighbourNext
                }
            }
            val neighbourPrev = byStartOffset[prev.endExclusive]
            if (neighbourPrev != null) {
                if (neighbourPrev.size >= minByteSize.size) {
                    return neighbourPrev
                }
            }
        }

        if (exactBlockSize != null) {
            if (size < exactBlockSize) {
                throw OutOfMemoryException()
            }

            val segments = bySize[exactBlockSize.size]
            if (!segments.isNullOrEmpty()) {
                return segments.last()
            }

            val sumExact = exactBlockSize.size + minByteSize.size
            for (entry in byStartOffset.entries) {
                if (entry.value.size >= sumExact) {
                    return entry.value
                }
            }
        } else {
            if (size < maxByteSize) {
                throw OutOfMemoryException()
            }

            val segments = bySize[maxByteSize.size]
            if (!segments.isNullOrEmpty()) {
                return segments.last()
            }

            val sumMax = maxByteSize.size + minByteSize.size
            for (entry in byStartOffset.entries) {
                if (entry.value.size >= sumMax) {
                    return entry.value
                }
            }

            val sumMin = minByteSize.size + minByteSize.size
            for (entry in byStartOffset.entries) {
                if (entry.value.size >= sumMin) {
                    return entry.value
                }
            }
        }

        throw OutOfMemoryException("Out of memory $size")
    }

    fun findByStartOffset(startOffset: Long): Block? {
        return byStartOffset[startOffset]
    }

    fun findByEndOffset(endOffset: Long): Block? {
        return byEndOffset[endOffset]
    }
}