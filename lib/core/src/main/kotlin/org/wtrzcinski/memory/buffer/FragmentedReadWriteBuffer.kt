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
import org.wtrzcinski.memory.bitmap.ReleaseResult
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler
import org.wtrzcinski.memory.mode.OpenMode
import org.wtrzcinski.memory.util.Check
import java.nio.ByteBuffer
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
class FragmentedReadWriteBuffer(
    val iterator: MemoryBlockIterator,
    private val onClose: (AbstractMemoryReadWriteBuffer) -> Unit = {},
    private val onRelease: (AbstractMemoryReadWriteBuffer) -> ReleaseResult = {
        ReleaseResult()
    },
) : AbstractMemoryReadWriteBuffer(
    close = {
        iterator.close()
        onClose.invoke(it)
    },
    release = {
        var result = iterator.release()
        result += onRelease.invoke(it)
        result
    }
) {

    private val position = AtomicLong(0)

    override fun all(): Collection<MemoryBlockReadWriteMapper> {
        return iterator.data
    }

    override fun clear() {
        TODO("Not yet implemented")
    }

    override fun onClose(onClose: (AbstractMemoryReadWriteBuffer) -> Unit): AbstractMemoryReadWriteBuffer {
        return FragmentedReadWriteBuffer(
            iterator = iterator,
            onClose = onClose,
            onRelease = this.onRelease,
        )
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

    override fun remaining(): DefaultBlockSize {
        return iterator.bodySize()
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

    private inline val offsetBytes: DefaultBlockSize get() = iterator.first.schemas.refHandler.byteSize()

    private fun sizeBytes(current: MemoryBlockReadWriteMapper): DefaultBlockSize {
        return current.schemas.sizeHandler.byteSize()
    }

    override tailrec fun readRef(): BlockAddress? {
        monitor.throwIfNotReadable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < offsetBytes) {
            val next = next()
            checkNotNull(next)
            return readRef()
        }
        position += offsetBytes.size
        return current.body.readRef()
    }

    override tailrec fun readSize(): DefaultBlockSize {
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
        if (remaining < SimpleVarHandler.longByteSize) {
            requireNotNull(next())
            return readLong()
        }
        position += SimpleVarHandler.longByteSize.size
        return current.body.readLong()
    }

    override tailrec fun readInt(): Int {
        monitor.throwIfNotReadable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < SimpleVarHandler.intByteSize) {
            requireNotNull(next())
            return readInt()
        }
        position += SimpleVarHandler.intByteSize.size
        return current.body.readInt()
    }

    override fun readShort(): Short {
        monitor.throwIfNotReadable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < SimpleVarHandler.shortByteSize) {
            requireNotNull(next())
            return readShort()
        }
        position += SimpleVarHandler.shortByteSize.size
        return current.body.readShort()
    }

    override fun readByte(): Byte {
        monitor.throwIfNotReadable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < SimpleVarHandler.byteSize) {
            requireNotNull(next())
            return readByte()
        }
        position += SimpleVarHandler.byteSize.size
        return current.body.readByte()
    }

    override fun read(dst: ByteBuffer, length: DefaultBlockSize): Int {
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

    override fun writeRef(value: BlockAddress): MemoryReadWriteBuffer {
        return write(iterator.first.schemas.refHandler, value)
    }

    override fun writeSize(value: DefaultBlockSize): MemoryReadWriteBuffer {
        return write(iterator.first.schemas.sizeHandler, value)
    }

    override tailrec fun <T: Any> write(handler: SimpleVarHandler<T>, value: T): MemoryReadWriteBuffer {
        monitor.throwIfNotWritable()

        val byteSize1 = handler.byteSize()
        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < byteSize1) {
            requireNotNull(next())
            return write(handler, value)
        }
        position += byteSize1.size
        handler.write(buffer = current.body, value = value)
        return this
    }

    override tailrec fun writeLong(value: Long) {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < SimpleVarHandler.longByteSize) {
            requireNotNull(next())
            return writeLong(value)
        }
        position += SimpleVarHandler.longByteSize.size
        current.body.writeLong(value)
    }

    override tailrec fun writeInt(value: Int) {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < SimpleVarHandler.intByteSize) {
            requireNotNull(next())
            return writeInt(value)
        }
        position += SimpleVarHandler.intByteSize.size
        current.body.writeInt(value)
    }

    override fun writeShort(value: Short) {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < SimpleVarHandler.shortByteSize) {
            requireNotNull(next())
            return writeShort(value)
        }
        position += SimpleVarHandler.shortByteSize.size
        current.body.writeShort(value)
    }

    override fun writeByte(value: Byte) {
        monitor.throwIfNotWritable()

        val current = iterator.current()
        checkNotNull(current)
        val remaining = current.body.remaining()
        if (remaining < SimpleVarHandler.byteSize) {
            requireNotNull(next())
            return writeByte(value)
        }
        position += SimpleVarHandler.byteSize.size
        current.body.writeByte(value)
    }

    override fun write(src: ByteBuffer, length: DefaultBlockSize): Int {
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

    override fun next(): MemoryBlockReadWriteMapper? {
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