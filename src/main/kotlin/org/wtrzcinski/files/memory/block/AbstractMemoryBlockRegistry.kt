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
import org.wtrzcinski.files.memory.ref.Block
import org.wtrzcinski.files.memory.ref.BlockStart
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.lock.ReadWriteMemoryFileLock
import java.lang.foreign.MemorySegment
import java.util.concurrent.ConcurrentHashMap

internal abstract class AbstractMemoryBlockRegistry(
    val memory: MemorySegment,
    val bitmap: BitmapRegistryGroup,
    val maxMemoryBlockSize: Long,
) : MemoryBlockRegistry {

    private val locks = ConcurrentHashMap<Long, ReadWriteMemoryFileLock>()

    private val minBodyByteSize: Long get() = MemoryBlockRegistry.longByteSize

    override val headerSize: Long get() = bodySizeHeaderSize + nextRefHeaderSize

    override val minMemoryBlockSize: Long get() = headerSize + minBodyByteSize

    override fun lock(offset: BlockStart): MemoryFileLock {
        return locks.compute(offset.start) { _, value ->
            return@compute value ?: ReadWriteMemoryFileLock(offset)
        } as MemoryFileLock
    }

    override fun findSegment(offset: BlockStart): MemoryBlock {
        require(offset.isValid())

        return MemoryBlock(segments = this, start = offset.start)
    }

    override fun releaseAll(other: Block) {
        bitmap.releaseAll(other = other)
    }

    override fun reserveSegment(prevOffset: Long, tag: String?): MemoryBlock {
        val bodyByteSize: Long = maxMemoryBlockSize - headerSize
        val segmentSize = bodyByteSize + headerSize
        val reserveBySize = bitmap.reserveBySize(
            byteSize = segmentSize,
            prev = prevOffset,
            tag = tag,
        )
        require(reserveBySize.start != prevOffset)
        return MemoryBlock(
            segments = this,
            start = reserveBySize.start,
            initialBodySize = bodyByteSize,
            initialNextRef = BlockStart.Invalid,
        )
    }
}