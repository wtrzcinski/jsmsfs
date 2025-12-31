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

package org.wtrzcinski.files.memory.allocator

import org.wtrzcinski.files.memory.MemorySegmentLedger
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.bitmap.BitmapRegistry
import org.wtrzcinski.files.memory.buffer.chunk.LongReadWriteBuffer
import org.wtrzcinski.files.memory.buffer.chunk.ChunkReadWriteBuffer
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.longByteSize
import java.lang.foreign.MemorySegment

internal class LongMemoryLedger(
    memory: MemorySegment,
    bitmap: BitmapRegistry,
    maxBlockByteSize: ByteSize,
) : MemorySegmentLedger(
    memory = memory,
    bitmap = bitmap,
    maxBlockSize = maxBlockByteSize,
) {

    override val sizeBytes: ByteSize = longByteSize

    override val offsetBytes: ByteSize = longByteSize

    init {
        require(maxBlockByteSize >= headerBytes)
    }

    override fun directBuffer(start: BlockStart, size: ByteSize): ChunkReadWriteBuffer {
        val asSlice: MemorySegment = this@LongMemoryLedger.memory.asSlice(start.start, size.size)
        return LongReadWriteBuffer(memorySegment = asSlice, release = { this.release(it.flip()) })
    }
}