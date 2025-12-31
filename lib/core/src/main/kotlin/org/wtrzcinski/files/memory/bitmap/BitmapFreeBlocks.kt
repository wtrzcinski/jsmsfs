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
import org.wtrzcinski.files.memory.exception.OptimisticLockException
import org.wtrzcinski.files.memory.exception.OutOfMemoryException
import org.wtrzcinski.files.memory.util.Check
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.minusAssign
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
class BitmapFreeBlocks {

    interface FindStrategy {
        fun find(free: BitmapFreeBlocks): Block
    }

    private val byStartOffset: MutableMap<Long, BitmapEntry> = ConcurrentHashMap()

    private val byEndOffset: MutableMap<Long, BitmapEntry> = ConcurrentHashMap()

    private val bySize: MutableMap<Long, CopyOnWriteArrayList<BitmapEntry>> = ConcurrentHashMap()

    private var freeSize: AtomicLong = AtomicLong(0L)

    val size: ByteSize
        get() {
            return ByteSize(freeSize.load())
        }

    fun findSizeSum(segmentSizeLt: ByteSize): Double {
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

        val next = findByStartOffset(current.end)
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
        this.byEndOffset[other.end] = other
        val bySizeList = this.bySize.computeIfAbsent(other.size) { CopyOnWriteArrayList() }
        bySizeList.add(other)
        return this
    }

    fun remove(other: Block) {
        this.byStartOffset.remove(other.start) ?: throw OptimisticLockException()
        this.byEndOffset.remove(other.end) ?: throw OptimisticLockException()
        val bySizeList = this.bySize[other.size] ?: throw OptimisticLockException()
        bySizeList.remove(other)
        if (bySizeList.isEmpty()) {
            this.bySize.remove(other.size)
        }
        this.freeSize -= other.size
    }

    fun findBySize(minByteSize: ByteSize, maxByteSize: ByteSize): BitmapEntry {
        Check.isTrue { minByteSize <= maxByteSize }

        if (size < maxByteSize) {
            throw OutOfMemoryException()
        }

        run {
            val segments = bySize[maxByteSize.size]
            if (!segments.isNullOrEmpty()) {
                return segments.last()
            }
        }

        run {
            val sum = (maxByteSize + minByteSize).size
            for (entry in bySize.entries) {
                if (entry.key >= sum) {
                    val segments = entry.value
                    if (segments.isNotEmpty()) {
                        return segments.last()
                    }
                }
            }
        }

        run {
            val sum = (minByteSize + minByteSize).size
            for (entry in bySize.entries) {
                if (entry.key >= sum) {
                    val segments = entry.value
                    if (segments.isNotEmpty()) {
                        return segments.last()
                    }
                }
            }
        }

        run {
            for (entry in bySize.entries) {
                if (entry.key >= minByteSize.size) {
                    val segments = entry.value
                    if (segments.isNotEmpty()) {
                        return segments.last()
                    }
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