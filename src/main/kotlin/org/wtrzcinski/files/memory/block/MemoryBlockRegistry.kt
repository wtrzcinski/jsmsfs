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
import org.wtrzcinski.files.memory.byteBuffer.MemoryBlockByteBuffer
import org.wtrzcinski.files.memory.ref.Block
import org.wtrzcinski.files.memory.ref.BlockStart
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import java.lang.foreign.MemorySegment

@Suppress("MayBeConstant")
internal interface MemoryBlockRegistry {

    val headerSize: Long

    val minMemoryBlockSize: Long

    val bodySizeHeaderSize: Long

    val nextRefHeaderSize: Long

    fun buffer(offset: Long, size: Long): MemoryBlockByteBuffer

    fun findSegment(offset: BlockStart): MemoryBlock

    fun reserveSegment(prevOffset: Long = MemoryBlockByteBuffer.invalidRef, tag: String? = null): MemoryBlock

    fun releaseAll(nodeRef: BlockStart) {
        val fileSegment = findSegment(nodeRef)
        fileSegment.use {
            fileSegment.release()
        }
    }

    fun releaseAll(other: Block)

    fun lock(offset: BlockStart): MemoryFileLock

    companion object {
        val byteByteSize: Long = Byte.SIZE_BYTES.toLong()
        val intByteSize: Long = Int.SIZE_BYTES.toLong()
        val longByteSize: Long = Long.SIZE_BYTES.toLong()

        operator fun invoke(memory: MemorySegment, bitmap: BitmapRegistryGroup, maxMemoryBlockByteSize: Long): MemoryBlockRegistry {
            if (memory.byteSize() <= MemoryBlockByteBuffer.maxUnsignedIntInclusive) {
                return IntMemoryBlockRegistry(
                    memory = memory,
                    bitmap = bitmap,
                    maxMemoryBlockByteSize = maxMemoryBlockByteSize,
                )
            } else {
                return LongMemoryBlockRegistry(
                    memory = memory,
                    bitmap = bitmap,
                    maxMemoryBlockByteSize = maxMemoryBlockByteSize,
                )
            }
        }
    }
}