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

package org.wtrzcinski.files.memory.buffer.channel

import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.buffer.chunk.ChunkReadWriteBuffer
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.mapper.MemoryBlockIterator
import org.wtrzcinski.files.memory.mapper.MemoryBlockReadWriteMapper
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry
import org.wtrzcinski.files.memory.mode.AbstractCloseable
import org.wtrzcinski.files.memory.util.Check
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
data class FragmentedReadWriteBuffer(
    val lock: MemoryFileLock?,
    val data: MemoryBlockIterator,
) : SeekableByteChannel, MemoryReadWriteBuffer, AbstractCloseable(), FragmentedReadBuffer {

    private val position = AtomicLong(0)

    val offsetBytes: ByteSize
        get() {
            return data.first().body.offsetBytes
        }

    fun count(): Int {
        return data.count()
    }

    fun get(index: Int): MemoryBlockReadWriteMapper {
        return data.get(index)
    }

    fun first(): MemoryBlockReadWriteMapper {
        return this.data.first()
    }

    override fun size(): Long {
        return data.bodySize().size
    }

    fun remaining(): ByteSize {
        return data.bodySize()
    }

    fun release() {
        checkIsClosed()

        if (tryRelease()) {
            data.release()
        } else {
            throwIllegalStateException()
        }
    }

    override fun close() {
        if (tryClose()) {
            data.close()

            lock?.release()
        }
    }

    fun flip(): BlockStart {
        Check.isTrue { position.load() != 0L }

        if (tryFlip()) {
            data.flip()

            position.exchange(0L)

            return data.first()
        } else {
            throwIllegalStateException()
        }
    }

    override fun position(): Long {
        return position.load()
    }

    override fun position(newPosition: Long): FragmentedReadWriteBuffer {
        if (newPosition != this.position.load()) {
            TODO("Not yet implemented")
        }
        return this
    }

    fun skipRemaining() {
        checkIsReadable()

        position += data.skipRemaining()
    }

    private fun offsetBytes(current: MemoryBlockReadWriteMapper): ByteSize {
        return current.body.offsetBytes
    }

    override tailrec fun readOffset(): BlockStart? {
        checkIsReadable()

        val current = data.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < offsetBytes(current)) {
            checkNotNull(next())
            return readOffset()
        }
        position += offsetBytes(current).size
        return current.body.readOffset()
    }

    override fun readSize(): ByteSize {
        checkIsReadable()

        TODO("Not yet implemented")
    }

    override fun read(other: ByteBuffer): Int {
        checkIsReadable()

        val length = other.remaining()
        val byteArray = ByteArray(length)
        val read = read(dst = byteArray)
        require(read <= length)
        if (read == 0) {
            return -1
        } else {
            other.put(byteArray, 0, read)
            return read
        }
    }

    override tailrec fun readLong(): Long {
        checkIsReadable()

        val current = data.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < MemoryMapperRegistry.longByteSize) {
            requireNotNull(next())
            return readLong()
        }
        position += MemoryMapperRegistry.longByteSize.size
        return current.body.readLong()
    }

    override fun readString(charset: Charset): String {
        checkIsReadable()

        return super<FragmentedReadBuffer>.readString(charset)
    }

    fun skipInt() {
        readOffset()
    }

    override tailrec fun readInt(): Int {
        checkIsReadable()

        val current = data.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < MemoryMapperRegistry.intByteSize) {
            requireNotNull(next())
            return readInt()
        }
        position += MemoryMapperRegistry.intByteSize.size
        return current.body.readInt()
    }

    override fun read(dst: ByteArray): Int {
        var dstOffset = 0
        val dstLength = ByteSize(dst.size)
        while (!Thread.currentThread().isInterrupted) {
            val current = data.current() ?: break
            val left = dstLength - dstOffset
            val remaining = current.body.remaining()
            if (remaining.isEmpty()) {
                next() ?: break
            } else if (remaining < left) {
                current.body.read(dst = dst, dstOffset = dstOffset, length = remaining)
                position += remaining.toLong()
                dstOffset += remaining.toInt()
                next() ?: break
            } else {
                current.body.read(dst = dst, dstOffset = dstOffset, length = left)
                position += left.toLong()
                dstOffset += left.toInt()
                break
            }
        }
        return dstOffset
    }

    override fun write(value: FragmentedReadWriteBuffer): Int {
        checkIsWritable()

        var count = 0
        for (mapper: MemoryBlockReadWriteMapper in value.data.data) {
            count += write(mapper.body)
        }

        return count
    }

    override fun write(buffer: ChunkReadWriteBuffer): Int {
        checkIsWritable()

        return write(buffer.byteBuffer)
    }

    override tailrec fun writeOffset(value: BlockStart) {
        checkIsWritable()

        val current = data.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < offsetBytes(current)) {
            requireNotNull(next())
            return writeOffset(value)
        }
        position += offsetBytes(current).size
        current.body.writeOffset(value)
    }

    override fun writeSize(value: ByteSize) {
        checkIsWritable()

        TODO("Not yet implemented")
    }

    override tailrec fun writeLong(value: Long) {
        checkIsWritable()

        val current = data.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < MemoryMapperRegistry.longByteSize) {
            requireNotNull(next())
            return writeLong(value)
        }
        position += MemoryMapperRegistry.longByteSize.size
        current.body.writeLong(value)
    }

    override tailrec fun writeInt(value: Int) {
        checkIsWritable()

        val current = data.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < MemoryMapperRegistry.intByteSize) {
            requireNotNull(next())
            return writeInt(value)
        }
        position += MemoryMapperRegistry.intByteSize.size
        current.body.writeInt(value)
    }

    override fun write(value: ByteArray) {
        checkIsWritable()

        write(other = ByteBuffer.wrap(value))
    }

    override fun write(other: ByteBuffer): Int {
        checkIsWritable()

        val current = data.current()
        checkNotNull(current)

        val currentRemaining = current.body.remaining()
        val otherRemaining = ByteSize(other.remaining())

        if (currentRemaining < otherRemaining) {
            current.body.write(other, currentRemaining)
            position += currentRemaining.toLong()
            requireNotNull(next())
            return currentRemaining.toInt() + write(other)
        } else {
            current.body.write(other, otherRemaining)
            position += otherRemaining.toLong()
            return otherRemaining.toInt()
        }
    }

    fun hasNext(): Boolean {
        return data.hasNext()
    }

    fun next(): MemoryBlockReadWriteMapper? {
        return data.next()
    }

    fun append(): FragmentedReadWriteBuffer {
        checkIsWritable()
        skipRemaining()
        return this
    }

    fun truncate(): FragmentedReadWriteBuffer {
        checkIsWritable()
        return truncate(0L)
    }

    override fun truncate(size: Long): FragmentedReadWriteBuffer {
        checkIsWritable()

        if (position() == 0L && size == 0L) {
            return this
        }
        TODO("Not yet implemented")
    }
}