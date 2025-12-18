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

package org.wtrzcinski.files.memory.channel

import org.wtrzcinski.files.memory.block.MemoryBlock
import org.wtrzcinski.files.memory.block.MemoryBlockIterator
import org.wtrzcinski.files.memory.block.MemoryBlockRegistry.Companion.intByteSize
import org.wtrzcinski.files.memory.block.MemoryBlockRegistry.Companion.longByteSize
import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.ref.BlockStart
import java.lang.AutoCloseable
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.StandardOpenOption
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE
import java.time.Instant
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
internal data class MemorySeekableByteChannel(
    val start: MemoryBlock,
    val lock: MemoryFileLock?,
    val mode: MemoryOpenOptions,
) : SeekableByteChannel, AutoCloseable {

    private var position = AtomicLong(0)

    private val closed = AtomicBoolean(false)

    private val segments = MemoryBlockIterator(start = start, mode = mode)

    fun offset(): BlockStart {
        return segments.offset()
    }

    override fun isOpen(): Boolean {
        return !closed.load()
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            try {
                if (this.mode.write) {
                    val current = segments.current()
                    val newBodySize = current.position
                    current.resize(newBodySize)
                }
                segments.close()
            } finally {
                lock?.release(mode)
            }
        }
    }

    override fun size(): Long {
        if (mode.read) {
            return segments.size()
        }
        TODO("Not yet implemented")
    }

    override fun position(): Long {
        return position.load()
    }

    override fun position(newPosition: Long): MemorySeekableByteChannel {
        if (newPosition != this.position.load()) {
            TODO("Not yet implemented")
        }
        return this
    }

    fun skipRemaining() {
        checkAccessible(READ)

        segments.skipRemaining()
    }

    fun readRef(): BlockStart {
        return BlockStart.of(readLong())
    }

    fun readInstant(): Instant {
        val epochSecond = readLong()
        val nano = readInt()
        return Instant.ofEpochSecond(epochSecond, nano.toLong())
    }

    fun readRefs(): Sequence<BlockStart> {
        val existing = mutableListOf<BlockStart>()
        val count = readInt()
        repeat(count) {
            existing.add(BlockStart.of(readLong()))
        }
        return existing.asSequence()
    }

    override fun read(other: ByteBuffer): Int {
        checkAccessible(READ)

        val length = other.remaining()
        val byteArray = ByteArray(length)
        val read = read(dst = byteArray, dstOffset = 0, dstLength = length)
        if (read == 0) {
            return -1
        } else {
            other.put(byteArray, 0, read)
            return read
        }
    }

    fun readLong(): Long {
        checkAccessible(READ)

        val current = segments.current()
        val remaining = current.remaining()
        if (remaining < longByteSize) {
            next()
            return readLong()
        }
        position += longByteSize
        return current.bodyBuffer.getLong()
    }

    fun readString(): String {
        checkAccessible(READ)

        val length = readInt()
        val dst = ByteArray(length)
        read(dst, 0, length)
        val result = String(dst)
        return result
    }

    fun readInt(): Int {
        checkAccessible(READ)

        val current = segments.current()
        val remaining = current.bodyBuffer.remaining()
        if (remaining < intByteSize) {
            next()
            return readInt()
        }
        position += intByteSize
        return current.bodyBuffer.getInt()
    }

    private fun read(dst: ByteArray, dstOffset: Int, dstLength: Int): Int {
        val current = segments.current()
        val left = dstLength - dstOffset
        val remaining = current.remaining()
        if (remaining == 0) {
            val next = next()
            if (!next) {
                return 0
            }
            val redNext = read(dst, dstOffset, dstLength)
            return redNext
        } else if (remaining < left) {
            current.bodyBuffer.get(dst, dstOffset, remaining)
            position += remaining.toLong()
            val next = next()
            if (!next) {
                return remaining
            }
            val redNext = read(dst, dstOffset + remaining, dstLength)
            return remaining + redNext
        } else {
            current.bodyBuffer.get(dst, dstOffset, left)
            position += left.toLong()
            return left
        }
    }

    fun writeRefs(list: Sequence<BlockStart>) {
        writeInt(list.count())
        for (ref in list) {
            writeLong(ref.start)
        }
    }

    override fun write(other: ByteBuffer): Int {
        checkAccessible(WRITE)

        val remaining = other.remaining()
        val byteArray = ByteArray(remaining)
        other.get(byteArray)
        write(byteArray = byteArray, offset = 0)
        return remaining
    }

    fun writeRef(ref: BlockStart) {
        writeLong(ref.start)
    }

    fun writeInstant(other: Instant) {
        val epochSecond = other.epochSecond
        val nano = other.nano
        writeLong(epochSecond)
        writeInt(nano)
    }

    fun writeLong(other: Long) {
        checkAccessible(WRITE)

        val current = segments.current()
        val remaining = current.bodyBuffer.remaining()
        if (remaining < longByteSize) {
            next()
            return writeLong(other)
        }
        position += longByteSize
        current.bodyBuffer.putLong(other)
    }

    fun writeMap(other: Map<String, String>) {
        writeInt(other.size)
        for ((key, _) in other) {
            writeString(key)
        }
        for ((_, value) in other) {
            writeString(value)
        }
    }

    fun writeList(other: List<String>) {
        writeInt(other.size)
        for (string in other) {
            writeString(string)
        }
    }

    fun writeInt(other: Int) {
        checkAccessible(WRITE)

        val current = segments.current()
        val remaining = current.bodyBuffer.remaining()
        if (remaining < intByteSize) {
            next()
            return writeInt(other)
        }
        position += intByteSize
        current.bodyBuffer.putInt(other)
    }

    fun writeString(other: String) {
        checkAccessible(WRITE)

        val name = other.toByteArray()
        writeInt(name.size)
        write(name, 0)
    }

    fun write(byteArray: ByteArray, offset: Int) {
        val current = segments.current()
        val toWrite = byteArray.size - offset
        val remaining = current.bodyBuffer.remaining()
        if (remaining < toWrite) {
            current.bodyBuffer.put(byteArray, offset, remaining)
            position += remaining.toLong()
            next()
            write(byteArray, offset + remaining)
        } else {
            current.bodyBuffer.put(byteArray, offset, toWrite)
            position += toWrite.toLong()
        }
    }

    private fun next(): Boolean {
        return segments.next() != null
    }

    override fun truncate(size: Long): MemorySeekableByteChannel {
        TODO("Not yet implemented")
    }

    private fun checkAccessible(option: StandardOpenOption) {
        if (!isOpen) {
            throw MemoryIllegalStateException("The byte channel is already closed.")
        }
        if (option == READ) {
            if (!mode.read) {
                throw MemoryIllegalStateException()
            }
        }
        if (option == WRITE) {
            if (!mode.write) {
                throw MemoryIllegalStateException()
            }
        }
    }
}