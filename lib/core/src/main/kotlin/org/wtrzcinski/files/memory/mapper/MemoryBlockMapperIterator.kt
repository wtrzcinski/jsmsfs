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

import org.wtrzcinski.files.memory.MemoryLedger
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryOpenOptions
import org.wtrzcinski.files.memory.util.AbstractCloseable
import org.wtrzcinski.files.memory.util.Preconditions.requireTrue
import java.lang.AutoCloseable
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
class MemoryBlockMapperIterator(
    val memory: MemoryLedger,
    val mode: MemoryOpenOptions,
    first: MemoryBlockReadWriteMapper,
    private val data: CopyOnWriteArrayList<MemoryBlockReadWriteMapper> = CopyOnWriteArrayList(),
    private val index: AtomicInt = AtomicInt(value = 0)
) : AutoCloseable, AbstractCloseable() {

    init {
        data.add(first)
    }

    fun first(): MemoryBlockReadWriteMapper {
        return data.first()
    }

    fun last(): MemoryBlockReadWriteMapper {
        return data.last()
    }

    fun bodySize(): ByteSize {
        var size = ByteSize(0)
        var current: MemoryBlockReadWriteMapper? = data.first()
        while (current != null) {
            size += current.readBodySize()
            current = readNext(current)
        }
        return size
    }

    fun skipRemaining(): Long {
        var remaining = 0L
        while (hasNext()) {
            remaining += skipNext()
        }
        val current = last()
        remaining += current.body.skipRemaining()
        return remaining
    }

    private fun skipNext(): Long {
        checkIsOpen()

        if (index.load() >= data.size) {
            return 0
        }

        val current = data.last()
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
        checkIsOpen()

        if (index.load() >= data.size) {
            return null
        }

        val current = data.last()
        val nextRef = readNext(current)
        if (nextRef != null) {
            if (this.mode.write) {
                val newBodySize = current.body.position()
                current.writeBodySize(ByteSize(newBodySize))
                data.add(nextRef)
                index += 1
                return nextRef
            } else {
                data.add(nextRef)
                index += 1
                return nextRef
            }
        } else {
            if (this.mode.write) {
                val newBodySize = current.body.position()
                current.writeBodySize(ByteSize(newBodySize))
                val nextRef = reserveNext(current)
                data.add(nextRef)
                index += 1
                return nextRef
            } else {
                index += 1
                return null
            }
        }
    }

    private fun readNext(current: MemoryBlockReadWriteMapper): MemoryBlockReadWriteMapper? {
        val offset = current.readNextOffset()
        if (offset != null && offset.isValid()) {
            requireTrue(mode.read)

            return MemoryBlockReadWriteMapper.existingBlock(memory = memory, offset = offset)
        }
        return null
    }

    private fun reserveNext(current: MemoryBlockReadWriteMapper): MemoryBlockReadWriteMapper {
        requireTrue(mode.write)

        val nextOffset = memory.newByteChannel(mode = mode, prev = current)
        val first = nextOffset.first()
        requireTrue(first.start != current.start)
        current.writeNextOffset(first)
        return first
    }

    fun hasNext(): Boolean {
        val current = data.last()
        val nextRef = current.readNextOffset()
        return nextRef != null
    }

    override fun close() {
        if (tryClose()) {
            if (mode.write) {
                val current = data.last()
                val newBodySize = current.body.position()
                current.writeBodySize(ByteSize(newBodySize))

                current.writeNextOffset(BlockStart.InvalidAddress)
            }
        }
    }
}