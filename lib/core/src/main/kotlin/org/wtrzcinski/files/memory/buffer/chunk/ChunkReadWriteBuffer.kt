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

package org.wtrzcinski.files.memory.buffer.chunk

import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.buffer.channel.FragmentedReadWriteBuffer
import org.wtrzcinski.files.memory.mapper.BlockBodyMapper
import org.wtrzcinski.files.memory.mode.AbstractCloseable
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer

@Suppress("UsePropertyAccessSyntax")
abstract class ChunkReadWriteBuffer(
    val memorySegment: MemorySegment,
    val byteBuffer: ByteBuffer,
    val release: (ChunkReadWriteBuffer) -> Unit = {},
) : AbstractCloseable(), MemoryReadWriteBuffer, BlockBodyMapper {

    abstract val offsetBytes: ByteSize

    fun release() {
        release(this)
    }

    fun skipRemaining(): Long {
        val remaining = byteBuffer.remaining()
        val limit = byteBuffer.limit()
        val position = byteBuffer.position()
        if (position != limit) {
            byteBuffer.position(limit)
        }
        return remaining.toLong()
    }

    /**
     * @see MemorySegment.force
     */
    fun flush() {
        if (!memorySegment.isReadOnly()) {
            if (memorySegment.isMapped()) {
                memorySegment.force()
            }
        }
    }

    /**
     * @see java.nio.Buffer.limit
     */
    fun limit(limit: ByteSize) {
        byteBuffer.limit(limit.toInt())
    }

    /**
     * @see java.nio.Buffer.rewind
     */
    fun rewind(): ByteBuffer {
        return byteBuffer.rewind()
    }

    /**
     * @see java.nio.Buffer.clear
     */
    fun clear() {
        byteBuffer.clear()
    }

    /**
     * @see java.nio.Buffer.flip
     */
    override fun flip(): BlockStart {
        if (tryFlip()) {
            check(byteBuffer.position() > 0)

            byteBuffer.flip()
            return BlockStart(offset = memorySegment.address())
        } else {
            throwIllegalStateException()
        }
    }

    /**
     * @see java.nio.Buffer.position
     */
    override fun position(): Long {
        return byteBuffer.position().toLong()
    }

    /**
     * @see java.nio.Buffer.remaining
     */
    fun remaining(): ByteSize {
        return ByteSize(value = byteBuffer.remaining())
    }

    /**
     * @see ByteBuffer.putInt
     */
    override fun writeInt(value: Int) {
        byteBuffer.putInt(value)
    }

    /**
     * @see ByteBuffer.putLong
     */
    override fun writeLong(value: Long) {
        byteBuffer.putLong(value)
    }

    /**
     * @see ByteBuffer.put
     */
    override fun write(value: ByteArray) {
        byteBuffer.put(value)
    }

    /**
     * @see ByteBuffer.put
     */
    override fun write(buffer: ChunkReadWriteBuffer): Int {
        val src = buffer.byteBuffer
        val remaining = src.remaining()
        byteBuffer.put(src)
        return remaining
    }

    /**
     * @see ByteBuffer.put
     */
    fun write(other: ByteBuffer, length: ByteSize) {
        val currentPosition = this.byteBuffer.position()
        val otherPosition = other.position()
        this.byteBuffer.put(currentPosition, other, otherPosition, length.toInt())
        this.byteBuffer.position(currentPosition + length.toInt())
        other.position(otherPosition + length.toInt())
    }

    override fun write(value: FragmentedReadWriteBuffer): Int {
        TODO("Not yet implemented")
    }

    /**
     * @see ByteBuffer.getInt
     */
    override fun readInt(): Int {
        val value = byteBuffer.getInt()
        return value
    }

    /**
     * @see ByteBuffer.getLong
     */
    override fun readLong(): Long {
        val value = byteBuffer.getLong()
        return value
    }

    /**
     * @see ByteBuffer.get
     */
    override fun read(dst: ByteArray): Int {
        byteBuffer.get(dst)
        return dst.size
    }

    /**
     * @see ByteBuffer.get
     */
    fun read(dst: ByteArray, dstOffset: Int, length: ByteSize) {
        byteBuffer.get(dst, dstOffset, length.toInt())
    }

    override fun close() {
        flush()
    }
}