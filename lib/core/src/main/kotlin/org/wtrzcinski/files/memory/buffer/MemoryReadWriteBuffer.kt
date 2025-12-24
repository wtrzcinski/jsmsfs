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
import org.wtrzcinski.files.memory.util.Preconditions.requireTrue
import java.nio.charset.Charset
import java.time.Instant

@Suppress("unused")
internal interface MemoryReadWriteBuffer : MemoryReadBuffer {
    companion object {
        //        -1L is reserved for invalid references
        val MaxUnsignedIntInclusive: Long = Integer.toUnsignedLong(-1) - 1L
        const val InvalidRef: Long = -1
    }

    fun position(): Long

    fun writeOffset(value: BlockStart)

    fun writeSize(value: ByteSize)

    fun writeLong(value: Long)

    fun writeInt(value: Int)

    fun write(value: ByteArray)

    fun write(value: MemoryByteBuffer): Int

    fun writeUnsignedInt(value: Long) {
        requireTrue(value >= 0)
        requireTrue(value <= MaxUnsignedIntInclusive)
        writeInt(value.toInt())
    }

    fun writeOffsets(value: Sequence<BlockStart>) {
        writeInt(value.count())
        for (ref in value) {
            writeOffset(ref)
        }
    }

    fun writeInstant(value: Instant) {
        writeLong(value.epochSecond)
        writeInt(value.nano)
    }

    fun writeString(value: String, charset: Charset = Charsets.UTF_8) {
        val byteArray = value.toByteArray(charset)
        writeInt(byteArray.size)
        write(byteArray)
    }

    fun writeMap(other: Map<String, String>, charset: Charset = Charsets.UTF_8) {
        writeInt(other.size)
        for ((key, value) in other) {
            writeString(key, charset)
            writeString(value, charset)
        }
    }

    fun writeList(other: List<String>, charset: Charset = Charsets.UTF_8) {
        writeInt(other.size)
        for (string in other) {
            writeString(string, charset)
        }
    }
}