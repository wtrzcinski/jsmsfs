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

import org.wtrzcinski.files.memory.MemoryLedger
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.bitmap.BitmapRegistry
import org.wtrzcinski.files.memory.buffer.IntMemoryByteBuffer
import org.wtrzcinski.files.memory.buffer.MemoryByteBuffer
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.intByteSize
import java.lang.foreign.MemorySegment

internal class IntMemoryLedger(
    memory: MemorySegment,
    bitmap: BitmapRegistry,
    maxBlockByteSize: ByteSize,
) : MemoryLedger(
    memory = memory,
    bitmap = bitmap,
    maxBlockSize = maxBlockByteSize
) {
    override val offsetBytes: ByteSize = intByteSize

    init {
        require(maxBlockByteSize >= headerBytes)
    }

    override fun directBuffer(start: BlockStart, size: ByteSize): MemoryByteBuffer {
        val asSlice: MemorySegment = this@IntMemoryLedger.memory.asSlice(start.start, size.size)
        return IntMemoryByteBuffer(memorySegment = asSlice, release = { this.release(it.flip()) })
    }

    override fun heapBuffer(size: ByteSize): MemoryByteBuffer {
        val segment = MemorySegment.ofArray(ByteArray(size.size.toInt()))
        return IntMemoryByteBuffer(memorySegment = segment, release = {})
    }
}