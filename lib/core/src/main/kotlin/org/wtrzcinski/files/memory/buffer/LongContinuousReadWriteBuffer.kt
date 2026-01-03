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

package org.wtrzcinski.files.memory.buffer

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry
import java.lang.foreign.MemorySegment

internal class LongContinuousReadWriteBuffer(
    memorySegment: MemorySegment,
    address: BlockOffset,
    close: (MemoryReadWriteBuffer) -> Unit = {},
    release: (MemoryReadWriteBuffer) -> Unit = {},
) : ContinuousReadWriteBuffer(
    memorySegment = memorySegment,
    address = address,
    byteBuffer = memorySegment.asByteBuffer(),
    close = close,
    release = release,
) {

    override val offsetBytes: ByteSize get() = MemoryMapperRegistry.longByteSize

    override val sizeBytes: ByteSize get() = MemoryMapperRegistry.longByteSize

    override fun onClose(close: (MemoryReadWriteBuffer) -> Unit): MemoryReadWriteBuffer {
        return LongContinuousReadWriteBuffer(memorySegment, this@LongContinuousReadWriteBuffer.address, close, release)
    }

    override fun readOffset(): BlockOffset? {
        val value = readLong()
        if (value == Block.InvalidRef) {
            return null
        }
        require(value >= 0)
        return BlockOffset.Companion(value)
    }

    override fun writeOffset(value: BlockOffset): MemoryReadWriteBuffer {
        if (!value.isValid()) {
            writeLong(Block.InvalidRef)
        } else {
            require(value.start >= 0)
            writeLong(value.start)
        }
        return this
    }

    override fun readSize(): ByteSize {
        val value = readLong()
        require(value >= 0)
        return ByteSize(value)
    }

    override fun writeSize(value: ByteSize): MemoryReadWriteBuffer {
        require(value.size >= 0)
        writeLong(value.size)
        return this
    }
}