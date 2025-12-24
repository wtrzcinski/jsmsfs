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
import org.wtrzcinski.files.memory.buffer.MemoryByteBuffer
import org.wtrzcinski.files.memory.buffer.MemoryReadBuffer
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.mapper.MemoryBlockMapperIterator
import org.wtrzcinski.files.memory.mapper.MemoryBlockReadWriteMapper
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry
import org.wtrzcinski.files.memory.util.AbstractCloseable
import java.lang.AutoCloseable
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
data class FragmentedReadWriteBuffer(
    val lock: MemoryFileLock?,
    val data: MemoryBlockMapperIterator,
) : SeekableByteChannel, AutoCloseable, MemoryReadWriteBuffer, AbstractCloseable(), FragmentedReadBuffer, MemoryReadBuffer {

    private val position = AtomicLong(0)

    val offsetBytes: ByteSize
        get() {
            return data.first().body.offsetBytes
        }

    fun first(): MemoryBlockReadWriteMapper {
        return this.data.first()
    }

    override fun size(): Long {
        return data.bodySize().size
    }

    override fun close() {
        if (tryClose()) {
            data.close()

//            Preconditions.assertTrue {
//                val position = position.load()
//                val size = data.bodySize().size
//                position == size
//            }

            lock?.release()
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
        checkIsOpen()
        check(data.mode.read)

        position += data.skipRemaining()
    }

    private fun offsetBytes(current: MemoryBlockReadWriteMapper): ByteSize {
        return current.body.offsetBytes
    }

    override fun readOffset(): BlockStart? {
        checkIsOpen()
        check(data.mode.read)

        val current = data.last()
        val remaining = current.body.remaining()
        if (remaining < offsetBytes(current)) {
            if (next() == null) {
                throw MemoryIllegalStateException()
            }
            return readOffset()
        }
        position += offsetBytes(current).size
        return current.body.readOffset()
    }

    override fun readSize(): ByteSize {
        checkIsOpen()
        check(data.mode.read)

        TODO("Not yet implemented")
    }

    override fun read(other: ByteBuffer): Int {
        checkIsOpen()
        check(data.mode.read)

        val length = other.remaining()
        val byteArray = ByteArray(length)
        val read = read(dst = byteArray)
        require(read <= other.remaining())
        if (read == 0) {
            return -1
        } else {
            other.put(byteArray, 0, read)
            return read
        }
    }

    override fun readLong(): Long {
        checkIsOpen()
        check(data.mode.read)

        val current = data.last()
        val remaining = current.body.remaining()
        if (remaining < MemoryMapperRegistry.longByteSize) {
            requireNotNull(next())
            return readLong()
        }
        position += MemoryMapperRegistry.longByteSize.size
        return current.body.readLong()
    }

    override fun readString(charset: Charset): String {
        checkIsOpen()
        check(data.mode.read)

        return super<MemoryReadBuffer>.readString(charset)
    }

    fun skipInt() {
        readOffset()
    }

    override fun readInt(): Int {
        checkIsOpen()
        check(data.mode.read)

        val current = data.last()
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
            val current = data.last()
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

    override fun write(value: MemoryByteBuffer): Int {
        checkIsOpen()
        check(data.mode.write)

        val remaining = value.remaining()
        val byteArray = ByteArray(remaining.toInt())
        value.read(byteArray)
        write(byteArray = byteArray, offset = 0)
        return remaining.toInt()
    }

    override fun write(other: ByteBuffer): Int {
        checkIsOpen()
        check(data.mode.write)

        val remaining = other.remaining()
        val byteArray = ByteArray(remaining)
        other.get(byteArray)
        write(byteArray = byteArray, offset = 0)
        return remaining
    }

    override fun writeOffset(value: BlockStart) {
        checkIsOpen()
        check(data.mode.write)

        val current = data.last()
        val remaining = current.body.remaining()
        if (remaining < offsetBytes(current)) {
            requireNotNull(next())
            return writeOffset(value)
        }
        position += offsetBytes(current).size
        current.body.writeOffset(value)
    }

    override fun writeSize(value: ByteSize) {
        checkIsOpen()
        check(data.mode.write)

        TODO("Not yet implemented")
    }

    override fun writeLong(value: Long) {
        checkIsOpen()
        check(data.mode.write)

        val current = data.last()
        val remaining = current.body.remaining()
        if (remaining < MemoryMapperRegistry.longByteSize) {
            requireNotNull(next())
            return writeLong(value)
        }
        position += MemoryMapperRegistry.longByteSize.size
        current.body.writeLong(value)
    }

    override fun writeInt(value: Int) {
        checkIsOpen()
        check(data.mode.write)

        val current = data.last()
        val remaining = current.body.remaining()
        if (remaining < MemoryMapperRegistry.intByteSize) {
            requireNotNull(next())
            return writeInt(value)
        }
        position += MemoryMapperRegistry.intByteSize.size
        current.body.writeInt(value)
    }

    override fun write(value: ByteArray) {
        write(byteArray = value, offset = 0)
    }

    private fun write(byteArray: ByteArray, offset: Int) {
        val current = data.last()
        val toWrite = byteArray.size - offset
        val remaining = current.body.remaining()
        if (remaining.toLong() < toWrite) {
            current.body.write(byteArray, offset, remaining.toInt())
            position += remaining.toLong()
            val nextOffset = (offset + remaining.toInt())
            requireNotNull(next())
            write(byteArray, nextOffset)
        } else {
            current.body.write(byteArray, offset, toWrite)
            position += toWrite.toLong()
        }
    }

    fun hasNext(): Boolean {
        return data.hasNext()
    }

    fun next(): MemoryBlockReadWriteMapper? {
        return data.next()
    }

    fun append(): FragmentedReadWriteBuffer {
        checkIsOpen()
        check(data.mode.write)

        skipRemaining()
        return this
    }

    fun truncate(): FragmentedReadWriteBuffer {
        checkIsOpen()
        check(data.mode.write)

        return truncate(0L)
    }

    override fun truncate(size: Long): FragmentedReadWriteBuffer {
        checkIsOpen()
        check(data.mode.write)

        if (position() == 0L && size == 0L) {
            return this
        }
        TODO("Not yet implemented")
    }
}