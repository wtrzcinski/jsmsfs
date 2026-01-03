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

import org.wtrzcinski.files.memory.address.Block.Companion.InvalidRef
import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.util.Check
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.time.Instant

@Suppress("unused")
sealed interface MemoryReadBuffer {

    companion object {
        //        -1L is reserved for invalid references
        val MaxUnsignedIntInclusive: Long = Integer.toUnsignedLong(-1) - 1L
        val unsignedIntRange: LongRange = 0..MaxUnsignedIntInclusive
    }

    fun readOffset(): BlockOffset?

    fun readSize(): ByteSize

    fun readLong(): Long

    fun readInt(): Int

    fun read(dst: ByteBuffer, length: ByteSize): Int

    fun read(dst: ByteArray): Int {
        val dst1 = ByteBuffer.wrap(dst)
        return read(dst1, ByteSize(dst1.remaining()))
    }

    fun readUnsignedInt(): Long? {
        val intValue = readInt()
        if (intValue == InvalidRef.toInt()) {
            return null
        }
        val value = Integer.toUnsignedLong(intValue)
        Check.isTrue { value in unsignedIntRange }
        return value
    }

    fun readRefs(): Sequence<BlockOffset> {
        val existing = mutableListOf<BlockOffset>()
        val count = readInt()
        repeat(count) {
            val element = readOffset()
            requireNotNull(element)
            existing.add(element)
        }
        return existing.asSequence()
    }

    fun readInstant(): Instant {
        val epochSecond = readLong()
        val nanoAdjustment = readInt()
        return Instant.ofEpochSecond(epochSecond, nanoAdjustment.toLong())
    }

    fun readString(charset: Charset = Charsets.UTF_8): String {
        val length = readInt()
        if (length == 0) {
            return ""
        }
        val dst = ByteBuffer.allocate(length)
        val read = read(dst, ByteSize(length))
        Check.isTrue { read == length }
        return String(dst.array(), charset)
    }

    fun skipRemaining(): Long

    fun skipInt() {
        readInt()
    }
}