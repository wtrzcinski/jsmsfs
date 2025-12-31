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
import org.wtrzcinski.files.memory.allocator.IntMemoryLedger.Companion.MaxUnsignedIntInclusive
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer.Companion.InvalidRef
import org.wtrzcinski.files.memory.util.Check
import java.nio.charset.Charset
import java.time.Instant

@Suppress("unused")
interface MemoryReadBuffer {

    fun readOffset(): BlockStart?

    fun readSize(): ByteSize

    fun readLong(): Long

    fun readInt(): Int

    fun read(dst: ByteArray): Int

    fun readUnsignedInt(): Long? {
        val intValue = readInt()
        if (intValue == InvalidRef.toInt()) {
            return null
        }
        val value = Integer.toUnsignedLong(intValue)
        Check.isTrue {
            val range: LongRange = 0..MaxUnsignedIntInclusive
            value in range
        }
        return value
    }

    fun readRefs(): Sequence<BlockStart> {
        val existing = mutableListOf<BlockStart>()
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
        val dst = ByteArray(length)
        val read = read(dst)
        Check.isTrue { read == length }
        val result = String(dst, charset)
        return result
    }

    fun readMap(charset: Charset = Charsets.UTF_8): Map<String, String> {
        val map = mutableMapOf<String, String>()
        val size = readInt()
        repeat(size) {
            val key = readString(charset)
            val value = readString(charset)
            map[key] = value
        }
        return map
    }

    fun readList(charset: Charset = Charsets.UTF_8): List<String> {
        val list = mutableListOf<String>()
        val size = readInt()
        repeat(size) {
            val key = readString(charset)
            list.add(key)
        }
        return list
    }
}