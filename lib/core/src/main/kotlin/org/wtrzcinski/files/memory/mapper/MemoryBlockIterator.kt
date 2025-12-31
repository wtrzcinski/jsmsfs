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

package org.wtrzcinski.files.memory.mapper

import org.wtrzcinski.files.memory.MemorySegmentLedger
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.mode.AbstractCloseable
import org.wtrzcinski.files.memory.mode.OpenMode
import org.wtrzcinski.files.memory.mode.OpenMode.ReadWrite
import org.wtrzcinski.files.memory.provider.MemoryFileOpenOptions
import org.wtrzcinski.files.memory.util.Check
import org.wtrzcinski.files.memory.util.HistoricalLog
import org.wtrzcinski.files.memory.util.Require
import java.lang.AutoCloseable
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
class MemoryBlockIterator(
    val name: String,
    val memory: MemorySegmentLedger,
    mode: MemoryFileOpenOptions,
    first: MemoryBlockReadWriteMapper,
    private val capacity: ByteSize = ByteSize.InvalidSize,
    val data: CopyOnWriteArrayList<MemoryBlockReadWriteMapper> = CopyOnWriteArrayList(),
    private val index: AtomicInt = AtomicInt(value = 0),
) : AutoCloseable, AbstractCloseable(mode = mode.mode) {

    init {
        data.add(first)
    }

    fun count(): Int {
        return data.count()
    }

    fun get(index: Int): MemoryBlockReadWriteMapper {
        return data[index]
    }

    fun first(): MemoryBlockReadWriteMapper {
        return data.first()
    }

    fun current(): MemoryBlockReadWriteMapper? {
        val index = index.load()
        if (index == data.size) {
            return null
        }
        return data[index]
    }

    fun bodySize(): ByteSize {
        checkIsReadable()

        var size = ByteSize(0)
        var current: MemoryBlockReadWriteMapper? = data.first()
        while (current != null) {
            size += current.readBodySize()
            current = readNext(current)
        }

        if (capacity.isValid()) {
            Check.isTrue { size <= capacity }
        }

        return size
    }

    fun skipRemaining(): Long {
        checkIsReadable()

        var remaining = 0L
        while (hasNext()) {
            remaining += skipNext()
        }

        val current = current()
        checkNotNull(current)
        remaining += current.body.skipRemaining()
        return remaining
    }

    private fun skipNext(): Long {
        checkIsReadable()

        if (index.load() >= data.size) {
            return 0
        }

        val current = current()
        checkNotNull(current)
        val nextRef = readNext(current)
        if (nextRef != null) {
            val remaining = nextRef.body.skipRemaining()
            data.add(nextRef)
            index += 1
            return remaining
        } else {
            index += 1
            return 0
        }
    }

    fun next(): MemoryBlockReadWriteMapper? {
        checkIsReadable()

        val current = current() ?: return null

        val readNextOffset = readNextOffset(current)
        if (readNextOffset != null) {
            if (openMode == ReadWrite) {
                check(index.load() == data.size - 1)

                val newBodySize = current.body.position()
                current.writeBodySizeAndTruncate(ByteSize(newBodySize))

                val nextRef = MemoryBlockReadWriteMapper.existingBlock(memory = memory, offset = readNextOffset)
                data.add(nextRef)
                index += 1
                return nextRef
            } else if (openMode == OpenMode.ReadOnly) {
                if (index.load() == data.size - 1) {
                    val nextRef = MemoryBlockReadWriteMapper.existingBlock(memory = memory, offset = readNextOffset)
                    data.add(nextRef)
                    index += 1
                    return nextRef
                } else {
                    index += 1
                    val reuse = data[index.load()]
                    return reuse
                }
            } else {
                Require.unsupported()
            }
        } else {
            if (openMode == ReadWrite) {
                check(index.load() == data.size - 1)

                val newBodySize = current.body.position()
                current.writeBodySizeAndTruncate(ByteSize(newBodySize))

                val nextRef = reserveNext(current)
                data.add(nextRef)
                index += 1
                return nextRef
            } else if (openMode == OpenMode.ReadOnly) {
                check(index.load() == data.size - 1)

                index += 1
                return null
            } else {
                Require.unsupported()
            }
        }
    }

    private fun readNextOffset(current: MemoryBlockReadWriteMapper): BlockStart? {
        checkIsReadable()

        val offset = current.readNextOffset()
        if (offset != null && offset.isValid()) {
            return offset
        }
        return null
    }

    private fun readNext(current: MemoryBlockReadWriteMapper): MemoryBlockReadWriteMapper? {
        checkIsReadable()

        val offset = current.readNextOffset()
        if (offset != null && offset.isValid()) {
            return MemoryBlockReadWriteMapper.existingBlock(memory = memory, offset = offset)
        }
        return null
    }

    private fun reserveNext(current: MemoryBlockReadWriteMapper): MemoryBlockReadWriteMapper {
        checkIsWritable()

        val nextOffset = memory.newBuffer(name = "$name.$index", prev = current)
        val first = nextOffset.first()
        Check.isTrue { first.start != current.start }
        current.writeNextOffsetAndRelease(first)
        return first
    }

    fun hasNext(): Boolean {
        val current = current()
        checkNotNull(current)
        val nextRef = current.readNextOffset()
        return nextRef != null
    }

    fun release() {
        checkIsClosed()

        if (tryRelease()) {
            for (mapper in data) {
                memory.release(mapper)
            }
        } else {
            throwIllegalStateException()
        }
    }

    override fun close() {
        val wasWritable = isWritable()
        if (tryClose()) {
            if (wasWritable) {
                truncate()
            }
        }
    }

    fun flip() {
        val wasWritable = isWritable()
        if (tryFlip()) {
            if (wasWritable) {
                truncate()
            }

            this.index.exchange(0)

            for (datum in this.data) {
                datum.body.flip()
            }
        } else {
            throwIllegalStateException()
        }
    }

    private fun truncate() {
        val current = current()
        checkNotNull(current)
        val newBodySize = current.body.position()

        HistoricalLog.debug(this) { "truncate: $name.$index $newBodySize" }

        current.writeBodySizeAndTruncate(ByteSize(newBodySize))
        current.writeNextOffsetAndRelease(BlockStart.InvalidAddress)
    }

    override fun toString(): String {
        return "${javaClass.simpleName}(name=$name, index=$index, data=$data)"
    }
}