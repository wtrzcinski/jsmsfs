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

package org.wtrzcinski.files.memory.byteBuffer

import org.wtrzcinski.files.memory.ref.BlockStart
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer

internal abstract class MemoryBlockByteBuffer(
    val memorySegment: MemorySegment,
    val byteBuffer: ByteBuffer,
) {
    companion object {
//        -1L is reserved for invalid references
        val maxUnsignedIntInclusive: Long = Integer.toUnsignedLong(-1) - 1L
        val invalidRef: Long = -1
    }

    fun skipRemaining() {
        val limit = byteBuffer.limit()
        val position = byteBuffer.position()
        if (position != limit) {
            byteBuffer.position(limit)
        }
    }

    abstract fun readRef(): BlockStart

    abstract fun writeRef(value: BlockStart)

    abstract fun writeSize(value: Long)

    fun rewind(): ByteBuffer {
        return byteBuffer.rewind()
    }

    fun clear() {
        byteBuffer.clear()
    }

    fun position(): Int {
        return byteBuffer.position()
    }

    fun remaining(): Int {
        return byteBuffer.remaining()
    }

    fun putUnsignedInt(value: Long) {
        require(value >= 0)
        require(value <= maxUnsignedIntInclusive)
        putInt(value.toInt())
    }

    fun putInt(value: Int): ByteBuffer {
        return byteBuffer.putInt(value)
    }

    fun putLong(value: Long): ByteBuffer {
        return byteBuffer.putLong(value)
    }

    fun put(byteArray: ByteArray, offset: Int, remaining: Int) {
        byteBuffer.put(byteArray, offset, remaining)
    }

    fun getUnsignedInt(): Long? {
        val intValue = getInt()
        if (intValue == invalidRef.toInt()) {
            return null
        }
        val value = Integer.toUnsignedLong(intValue)
        require(value >= 0)
        require(value <= maxUnsignedIntInclusive)
        return value
    }

    fun getInt(): Int {
        return byteBuffer.getInt()
    }

    fun getInt(position: Int): Int {
        return byteBuffer.getInt(position)
    }

    fun getLong(): Long {
        return byteBuffer.getLong()
    }

    fun get(dst: ByteArray, dstOffset: Int, length: Int) {
        byteBuffer.get(dst, dstOffset, length)
    }
}