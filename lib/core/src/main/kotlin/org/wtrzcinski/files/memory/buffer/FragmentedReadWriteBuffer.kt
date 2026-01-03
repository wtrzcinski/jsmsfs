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

import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.mapper.MemoryBlockIterator
import org.wtrzcinski.files.memory.mapper.MemoryBlockReadWriteMapper
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry
import org.wtrzcinski.files.memory.mode.ModeState
import org.wtrzcinski.files.memory.util.Check
import java.nio.ByteBuffer
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
class FragmentedReadWriteBuffer(
    val data: MemoryBlockIterator,
    close: (MemoryReadWriteBuffer) -> Unit = {},
    release: (MemoryReadWriteBuffer) -> Unit = {},
) : MemoryReadWriteBuffer(close = close, release = release) {

    private val monitor = ModeState()

    private val position = AtomicLong(0)

    override fun clear() {
        TODO("Not yet implemented")
    }

    override fun onClose(close: (MemoryReadWriteBuffer) -> Unit): MemoryReadWriteBuffer {
        return FragmentedReadWriteBuffer(data, close, release)
    }

    override fun isOpen(): Boolean {
        return monitor.isOpen()
    }

    override fun count(): Int {
        return data.count()
    }

    fun get(index: Int): MemoryBlockReadWriteMapper {
        return data.get(index)
    }

    override fun address(): BlockOffset {
        return data.offset()
    }

    override fun size(): Long {
        return data.bodySize().size
    }

    override fun remaining(): ByteSize {
        return data.bodySize()
    }

    override fun release() {
        monitor.checkIsClosed()

        if (monitor.tryRelease()) {
            data.release()
            super.release()
        } else {
            monitor.throwIllegalStateException()
        }
    }

    override fun close() {
        if (monitor.tryClose()) {
            data.close()
            super.close()
        }
    }

    override fun flip(): BlockOffset {
        Check.isTrue { position.load() != 0L }

        if (monitor.tryFlip()) {
            data.flip()

            position.exchange(0L)

            return data.offset()
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
        monitor.checkIsReadable()

        val remaining = data.skipRemaining()
        position += remaining
        return remaining
    }

    override val offsetBytes: ByteSize get() = data.first.body.offsetBytes

    override val sizeBytes: ByteSize get() = data.first.body.sizeBytes

    private fun sizeBytes(current: MemoryBlockReadWriteMapper): ByteSize {
        return current.body.sizeBytes
    }

    override tailrec fun readOffset(): BlockOffset? {
        monitor.checkIsReadable()

        val current = data.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < offsetBytes) {
            checkNotNull(next())
            return readOffset()
        }
        position += offsetBytes.size
        return current.body.readOffset()
    }

    override tailrec fun readSize(): ByteSize {
        monitor.checkIsReadable()

        val current = data.current()
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
        monitor.checkIsReadable()

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

    override tailrec fun readInt(): Int {
        monitor.checkIsReadable()

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

    override fun read(dst: ByteBuffer, length: ByteSize): Int {
        monitor.checkIsReadable()

        var dstOffset = 0
        while (true) {
            val current = data.current() ?: break
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

    override tailrec fun writeOffset(value: BlockOffset): MemoryReadWriteBuffer {
        monitor.checkIsWritable()

        val current = data.current()
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
        monitor.checkIsWritable()

        val current = data.current()
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
        monitor.checkIsWritable()

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
        monitor.checkIsWritable()

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

    override fun write(src: ByteBuffer): Int {
        monitor.checkIsWritable()

        val otherRemaining = ByteSize(src.remaining())

        val current = data.current()
        checkNotNull(current)

        val currentRemaining = current.body.remaining()
        if (currentRemaining < otherRemaining) {
            current.body.write(src, currentRemaining)
            position += currentRemaining.toLong()
            requireNotNull(next())
            return currentRemaining.toInt() + write(src)
        } else {
            current.body.write(src, otherRemaining)
            position += otherRemaining.toLong()
            return otherRemaining.toInt()
        }
    }

    override fun write(src: ByteBuffer, length: ByteSize) {
        TODO("Not yet implemented")
    }

    fun hasNext(): Boolean {
        return data.hasNext()
    }

    fun next(): MemoryBlockReadWriteMapper? {
        return data.next()
    }

    override fun append(): FragmentedReadWriteBuffer {
        monitor.checkIsWritable()
        skipRemaining()
        return this
    }

    override fun truncate(): FragmentedReadWriteBuffer {
        return truncate(0L)
    }

    override fun truncate(size: Long): FragmentedReadWriteBuffer {
        monitor.checkIsWritable()

        if (position() == 0L && size == 0L) {
            return this
        }
        TODO("Not yet implemented")
    }
}