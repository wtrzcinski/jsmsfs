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
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.MaxUnsignedIntInclusive
import org.wtrzcinski.memory.util.Check
import java.nio.ByteBuffer
import java.time.Instant

sealed interface MemoryWriteBuffer {

    fun writeRef(value: BlockAddress): MemoryWriteBuffer

    fun writeSize(value: DefaultBlockSize): MemoryWriteBuffer

    fun writeLong(value: Long)

    fun writeInt(value: Int)

    fun writeShort(value: Short)

    fun writeByte(value: Byte)

    fun write(src: ByteBuffer, length: DefaultBlockSize): Int

    fun write(src: ByteBuffer): Int {
        return write(src, DefaultBlockSize(src.remaining()))
    }

    fun write(value: ByteArray) {
        write(src = ByteBuffer.wrap(value))
    }

    fun write(source: AbstractMemoryReadWriteBuffer): Int {
        when (source) {
            is ContinuousReadWriteBuffer -> {
                return write(source.byteBuffer)
            }

            is FragmentedReadWriteBuffer -> {
                var count = 0
                for (mapper: MemoryBlockReadWriteMapper in source.iterator.data) {
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

    fun writeUnsignedShort(value: Long) {
        Check.isTrue { value >= 0 }
        Check.isTrue { value <= MaxUnsignedIntInclusive }
        writeShort(value.toShort())
    }
}