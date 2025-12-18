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

package org.wtrzcinski.files.memory.block

import org.wtrzcinski.files.memory.bitmap.BitmapRegistryGroup
import org.wtrzcinski.files.memory.byteBuffer.IntMemoryBlockByteBuffer
import org.wtrzcinski.files.memory.byteBuffer.MemoryBlockByteBuffer
import java.lang.foreign.MemorySegment

internal class IntMemoryBlockRegistry(
    memory: MemorySegment,
    bitmap: BitmapRegistryGroup,
    maxMemoryBlockByteSize: Long,
) : AbstractMemoryBlockRegistry(
    memory = memory,
    bitmap = bitmap,
    maxMemoryBlockSize = maxMemoryBlockByteSize
) {
    override val bodySizeHeaderSize: Long = MemoryBlockRegistry.Companion.intByteSize

    override val nextRefHeaderSize: Long = MemoryBlockRegistry.Companion.intByteSize

    override fun buffer(offset: Long, size: Long): MemoryBlockByteBuffer {
        val asSlice: MemorySegment = memory.asSlice(offset, size)
        return IntMemoryBlockByteBuffer(memorySegment = asSlice)
    }
}