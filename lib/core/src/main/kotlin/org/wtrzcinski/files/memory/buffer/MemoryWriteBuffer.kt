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

import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryReadBuffer.Companion.MaxUnsignedIntInclusive
import org.wtrzcinski.files.memory.mapper.MemoryBlockReadWriteMapper
import org.wtrzcinski.files.memory.util.Check
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.time.Instant

sealed interface MemoryWriteBuffer {

    fun writeOffset(value: BlockOffset): MemoryWriteBuffer

    fun writeSize(value: ByteSize): MemoryWriteBuffer

    fun writeLong(value: Long)

    fun writeInt(value: Int)

    fun write(src: ByteBuffer): Int

    fun write(src: ByteBuffer, length: ByteSize)

    fun write(value: ByteArray) {
        write(src = ByteBuffer.wrap(value))
    }

    fun write(source: MemoryReadWriteBuffer): Int {
        when (source) {
            is ContinuousReadWriteBuffer -> {
                return write(source.byteBuffer)
            }

            is FragmentedReadWriteBuffer -> {
                var count = 0
                for (mapper: MemoryBlockReadWriteMapper in source.data.data) {
                    count += write(source = mapper.body)
                }
                return count
            }
        }
    }

    fun writeUnsignedInt(value: Long) {
        Check.isTrue { value >= 0 }
        Check.isTrue { value <= MaxUnsignedIntInclusive }
        writeInt(value.toInt())
    }

    fun writeOffsets(value: Sequence<BlockOffset>) {
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
        if (value.isNotEmpty()) {
            val byteArray = value.toByteArray(charset)
            writeInt(byteArray.size)
            write(byteArray)
        } else {
            writeInt(0)
        }
    }
}