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
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer.Companion.InvalidRef
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.longByteSize
import java.lang.foreign.MemorySegment

internal class LongMemoryByteBuffer(
    memorySegment: MemorySegment,
    release: (MemoryByteBuffer) -> Unit = {},
) : MemoryByteBuffer(
    memorySegment = memorySegment,
    byteBuffer = memorySegment.asByteBuffer(),
    release = release,
) {

    override val offsetBytes: ByteSize get() = longByteSize

    override fun readOffset(): BlockStart? {
        val value = readLong()
        if (value == InvalidRef) {
            return null
        }
        require(value >= 0)
        return BlockStart(value)
    }

    override fun writeOffset(value: BlockStart) {
        if (!value.isValid()) {
            writeLong(InvalidRef)
        } else {
            require(value.start >= 0)
            writeLong(value.start)
        }
    }

    override fun readSize(): ByteSize {
        val value = readLong()
        require(value >= 0)
        return ByteSize(value)
    }

    override fun writeSize(value: ByteSize) {
        require(value.size >= 0)
        writeLong(value.size)
    }
}