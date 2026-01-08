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

package org.wtrzcinski.files.memory.buffer

import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.mapper.MemoryBlockReadWriteMapper
import org.wtrzcinski.files.memory.mode.ModeMonitor
import org.wtrzcinski.files.memory.mode.OpenMode
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.byteSize
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.intByteSize
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.longByteSize
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.shortByteSize
import org.wtrzcinski.files.memory.util.Check
import java.nio.ByteBuffer
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
class FragmentedReadWriteBuffer(
    val iterator: MemoryBlockIterator,
    close: (MemoryReadWriteBuffer) -> Unit = {},
    release: (MemoryReadWriteBuffer) -> Unit = {},
) : MemoryReadWriteBuffer(close = close, release = release) {

    private val monitor = ModeMonitor()

    private val position = AtomicLong(0)

    override fun clear() {
        TODO("Not yet implemented")
    }

    override fun onClose(close: (MemoryReadWriteBuffer) -> Unit): MemoryReadWriteBuffer {
        return FragmentedReadWriteBuffer(iterator, close, release)
    }

    override fun isOpen(): Boolean {
        return monitor.isOpen()
    }

    override fun count(): Int {
        return iterator.count()
    }

    fun get(index: Int): MemoryBlockReadWriteMapper {
        return iterator.get(index)
    }

    override fun address(): BlockAddress {
        return iterator.offset()
    }

    override fun size(): Long {
        return iterator.bodySize().size
    }

    override fun remaining(): ByteSize {
        return iterator.bodySize()
    }

    override fun release() {
        monitor.throwIfNotClosed()

        if (monitor.tryRelease()) {
            iterator.release()
            super.release()
        } else {
            monitor.throwIllegalStateException()
        }
    }

    override fun close() {
        if (monitor.tryClose()) {
            iterator.close()
            super.close()
        }
    }

    override fun flip(mode: OpenMode): BlockAddress {
        Check.isTrue { position.load() != 0L }

        if (monitor.tryFlip()) {
            iterator.flip()

            position.exchange(0L)

            return iterator.offset()
        } else {
            monitor.throwIllegalStateException()
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

    override fun skipRemaining(): Long {
        monitor.throwIfNotReadable()

        val remaining = iterator.skipRemaining()
        position += remaining
        return remaining
    }

    override val offsetBytes: ByteSize get() = iterator.first.body.offsetBytes

    override val sizeBytes: ByteSize get() = iterator.first.body.sizeBytes

    private fun sizeBytes(current: MemoryBlockReadWriteMapper): ByteSize {
        return current.body.sizeBytes
    }

    override tailrec fun readOffset(): BlockAddress? {
        monitor.throwIfNotReadable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < offsetBytes) {
            val next = next()
            checkNotNull(next)
            return readOffset()
        }
        position += offsetBytes.size
        return current.body.readOffset()
    }

    override tailrec fun readSize(): ByteSize {
        monitor.throwIfNotReadable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < sizeBytes(current)) {
            checkNotNull(next())
            return readSize()
        }
        position += sizeBytes(current).size
        return current.body.readSize()
    }

    override tailrec fun readLong(): Long {
        monitor.throwIfNotReadable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < longByteSize) {
            requireNotNull(next())
            return readLong()
        }
        position += longByteSize.size
        return current.body.readLong()
    }

    override tailrec fun readInt(): Int {
        monitor.throwIfNotReadable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < intByteSize) {
            requireNotNull(next())
            return readInt()
        }
        position += intByteSize.size
        return current.body.readInt()
    }

    override fun readShort(): Short {
        monitor.throwIfNotReadable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < shortByteSize) {
            requireNotNull(next())
            return readShort()
        }
        position += shortByteSize.size
        return current.body.readShort()
    }

    override fun read(dst: ByteBuffer, length: ByteSize): Int {
        monitor.throwIfNotReadable()

        var dstOffset = 0
        while (true) {
            val current = iterator.current() ?: break
            val left = length - dstOffset
            val remaining = current.body.remaining()
            if (remaining.isEmpty()) {
                next() ?: break
            } else if (remaining < left) {
                current.body.read(dst = dst, length = remaining)
                position += remaining.toLong()
                dstOffset += remaining.toInt()
                next() ?: break
            } else {
                current.body.read(dst = dst, length = left)
                position += left.toLong()
                dstOffset += left.toInt()
                break
            }
        }
        require(dstOffset <= length.toInt())
        return if (dstOffset == 0) {
            -1
        } else {
            dstOffset
        }
    }

    override tailrec fun writeOffset(value: BlockAddress): MemoryReadWriteBuffer {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < offsetBytes) {
            requireNotNull(next())
            return writeOffset(value)
        }
        position += offsetBytes.size
        current.body.writeOffset(value)
        return this
    }

    override tailrec fun writeSize(value: ByteSize): MemoryReadWriteBuffer {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < sizeBytes(current)) {
            requireNotNull(next())
            return writeSize(value)
        }
        position += sizeBytes(current).size
        current.body.writeSize(value)
        return this
    }

    override tailrec fun writeLong(value: Long) {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < longByteSize) {
            requireNotNull(next())
            return writeLong(value)
        }
        position += longByteSize.size
        current.body.writeLong(value)
    }

    override tailrec fun writeInt(value: Int) {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < intByteSize) {
            requireNotNull(next())
            return writeInt(value)
        }
        position += intByteSize.size
        current.body.writeInt(value)
    }

    override fun writeShort(value: Short) {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < shortByteSize) {
            requireNotNull(next())
            return writeShort(value)
        }
        position += shortByteSize.size
        current.body.writeShort(value)
    }

    override fun writeByte(value: Byte) {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < byteSize) {
            requireNotNull(next())
            return writeByte(value)
        }
        position += byteSize.size
        current.body.writeByte(value)
    }

    override fun write(src: ByteBuffer, length: ByteSize): Int {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)

        val currentRemaining = current.body.remaining()
        if (currentRemaining < length) {
            current.body.write(src, currentRemaining)
            position += currentRemaining.toLong()
            requireNotNull(next())
            return currentRemaining.toInt() + write(src)
        } else {
            current.body.write(src, length)
            position += length.toLong()
            return length.toInt()
        }
    }

    fun hasNext(): Boolean {
        return iterator.hasNext()
    }

    fun next(): MemoryBlockReadWriteMapper? {
        return iterator.next()
    }

    override fun append(): FragmentedReadWriteBuffer {
        monitor.throwIfNotWritable()
        skipRemaining()
        return this
    }

    override fun truncate(): FragmentedReadWriteBuffer {
        return truncate(0L)
    }

    override fun truncate(size: Long): FragmentedReadWriteBuffer {
        monitor.throwIfNotWritable()

        if (position() == 0L && size == 0L) {
            return this
        }
        TODO("Not yet implemented")
    }
}