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

package org.wtrzcinski.memory.buffer

import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.address.DefaultBlockAddress
import org.wtrzcinski.memory.bitmap.ReleaseResult
import org.wtrzcinski.memory.mode.Mode
import org.wtrzcinski.memory.mode.ModeMonitor
import org.wtrzcinski.memory.mode.OpenMode
import org.wtrzcinski.memory.util.Require
import java.lang.AutoCloseable
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.minusAssign
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
@Suppress("unused")
class MemoryBlockIterator(
    val name: String = "",
    val memory: BufferAllocator,
    mode: Mode,
    val first: MemoryBlockReadWriteMapper,
    val data: CopyOnWriteArrayList<MemoryBlockReadWriteMapper> = CopyOnWriteArrayList(),
    private val index: AtomicInt = AtomicInt(value = -1),
) : AutoCloseable, ModeMonitor(mode = mode) {

    init {
        init()
    }

    fun count(): Int {
        return data.count()
    }

    fun get(index: Int): MemoryBlockReadWriteMapper {
        return data[index]
    }

    fun offset(): BlockAddress {
        return DefaultBlockAddress(first)
    }

    fun isInitialized(): Boolean {
        return index.load() >= 0
    }

    fun current(): MemoryBlockReadWriteMapper? {
        init()
        if (index.load() == data.size) {
            return null
        }
        return data[index.load()]
    }

    fun bodySize(): DefaultBlockSize {
        throwIfNotReadable()
        init()
        var size = DefaultBlockSize(0)
        var current: MemoryBlockReadWriteMapper? = data.first()
        while (current != null) {
            size += current.readBodySize()
            current = readNext(current)
        }

        return size
    }

    private fun init() {
        if (index.load() == -1) {
            next()
        }
    }

    fun skipRemaining(): Long {
        throwIfNotReadable()

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
        throwIfNotReadable()

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

    fun prev(): MemoryBlockReadWriteMapper? {
        throwIfNotReadable()

        if (index.load() > 0) {
            index -= 1
            return data[index.load()]
        } else if (index.load() == 0) {
            return null
        }

        Require.unsupported()
    }

    fun next(): MemoryBlockReadWriteMapper? {
        throwIfNotReadable()

        if (index.load() == -1) {
            index += 1
            data.add(first)
            return first
        }

        val current = current() ?: return null

        val readNextOffset = readNextOffset(current)
        if (readNextOffset != null) {
            when (openMode) {
                OpenMode.Post -> {
                    check(index.load() == data.size - 1)

                    val newBodySize = current.body.position()
                    current.writeBodySizeAndRelease(DefaultBlockSize(newBodySize))

                    val nextRef = memory.existingBuffer(ref = readNextOffset)
                    data.add(nextRef)
                    index += 1
                    return nextRef
                }
                OpenMode.Put, OpenMode.Get -> {
                    if (index.load() == data.size - 1) {
                        val nextRef = memory.existingBuffer(ref = readNextOffset)
                        data.add(nextRef)
                        index += 1
                        return nextRef
                    } else {
                        index += 1
                        val reuse = data[index.load()]
                        return reuse
                    }
                }
                else -> {
                    Require.unsupported()
                }
            }
        } else {
            when (openMode) {
                OpenMode.Post -> {
                    check(index.load() == data.size - 1)

                    val newBodySize = current.body.position()
                    current.writeBodySizeAndRelease(DefaultBlockSize(newBodySize))

                    val position = current.body.position()
                    val nextRef = memory.allocateBuffer(first = BlockAddress(first.start), prev = current)
                    if (nextRef.start != current.start) {
                        current.writeNextAddressAndRelease(nextRef)
                        data.add(nextRef)
                        index += 1
                    } else {
                        data.remove(current)
                        nextRef.body.position(position)
                        data.add(nextRef)
                    }
                    return nextRef
                }
                OpenMode.Put, OpenMode.Get -> {
                    check(index.load() == data.size - 1)

                    index += 1
                    return null
                }
                else -> {
                    Require.unsupported()
                }
            }
        }
    }

    private fun readNextOffset(current: MemoryBlockReadWriteMapper): BlockAddress? {
        throwIfNotReadable()

        val offset = current.readNextAddress()
        if (offset != null && offset.isValid()) {
            return offset
        }
        return null
    }

    private fun readNext(current: MemoryBlockReadWriteMapper): MemoryBlockReadWriteMapper? {
        throwIfNotReadable()

        val offset = current.readNextAddress()
        if (offset != null && offset.isValid()) {
            return memory.existingBuffer(ref = offset)
        }
        return null
    }

    fun hasNext(): Boolean {
        val current = current()
        checkNotNull(current)
        val nextRef = current.readNextAddress()
        return nextRef != null
    }

    fun release(): ReleaseResult {
        throwIfNotClosed()

        if (tryRelease()) {
            var result = ReleaseResult()
            for (mapper in data) {
                result += memory.releaseOne(mapper)
            }
            return result
        } else {
            throwIllegalStateException()
        }
    }

    override fun close() {
        val wasCreate = isCreating()
        if (tryClose()) {
            if (wasCreate) {
                truncate()
            }
        }
    }

    fun flip() {
        val wasCreate = isCreating()
        if (tryFlip()) {
            if (wasCreate) {
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

        current.writeBodySizeAndRelease(DefaultBlockSize(newBodySize))
        current.writeNextAddressAndRelease(BlockAddress.InvalidAddress)
    }

    override fun toString(): String {
        return "${javaClass.simpleName}(name=$name, index=$index, data=$data)"
    }

}