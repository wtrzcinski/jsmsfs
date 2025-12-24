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

package org.wtrzcinski.files.memory.buffer

import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.mapper.BlockBodyMapper
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer

@Suppress("UsePropertyAccessSyntax")
abstract class MemoryByteBuffer(
    val memorySegment: MemorySegment,
    val byteBuffer: ByteBuffer,
    val release: (MemoryByteBuffer) -> Unit = {},
) : AutoCloseable, MemoryReadWriteBuffer, BlockBodyMapper {

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
    fun force() {
        if (!memorySegment.isReadOnly()) {
            if (memorySegment.isMapped()) {
                memorySegment.force()
            }
        }
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
        byteBuffer.flip()
        return BlockStart(offset = memorySegment.address())
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
        return ByteSize(value = byteBuffer.remaining().toLong())
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
    override fun write(value: MemoryByteBuffer): Int {
        val src = value.byteBuffer
        val remaining = src.remaining()
        byteBuffer.put(src)
        return remaining
    }

    /**
     * @see ByteBuffer.put
     */
    fun write(byteArray: ByteArray, offset: Int, remaining: Int) {
        byteBuffer.put(byteArray, offset, remaining)
    }

    /**
     * @see ByteBuffer.getInt
     */
    override fun readInt(): Int {
        return byteBuffer.getInt()
    }

    /**
     * @see ByteBuffer.getLong
     */
    override fun readLong(): Long {
        return byteBuffer.getLong()
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
        force()
    }
}