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

package org.wtrzcinski.files.memory

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.allocator.IntMemoryLedger
import org.wtrzcinski.files.memory.allocator.LongMemoryLedger
import org.wtrzcinski.files.memory.bitmap.BitmapEntry
import org.wtrzcinski.files.memory.bitmap.BitmapRegistry
import org.wtrzcinski.files.memory.buffer.MemoryByteBuffer
import org.wtrzcinski.files.memory.buffer.MemoryOpenOptions
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.buffer.channel.FragmentedReadWriteBuffer
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.mapper.MemoryBlockMapperIterator
import org.wtrzcinski.files.memory.mapper.MemoryBlockReadWriteMapper
import org.wtrzcinski.files.memory.util.Preconditions.assertTrue
import java.lang.foreign.MemorySegment
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
abstract class MemoryLedger(
    val memory: MemorySegment,
    val bitmap: BitmapRegistry,
    val maxBlockSize: ByteSize,
) {

    open val sizeBytes: ByteSize get() = offsetBytes

    abstract val offsetBytes: ByteSize

    val headerBytes: ByteSize get() = sizeBytes + offsetBytes

    abstract fun directBuffer(start: BlockStart, size: ByteSize): MemoryByteBuffer

    abstract fun heapBuffer(size: ByteSize): MemoryByteBuffer

    fun existingByteChannel(
        mode: MemoryOpenOptions,
        offset: BlockStart,
        lock: MemoryFileLock? = null
    ): FragmentedReadWriteBuffer {
        assertTrue(offset.isValid())

        val first = MemoryBlockReadWriteMapper.existingBlock(memory = this, offset = offset)
        val channel = FragmentedReadWriteBuffer(
            lock = lock,
            data = MemoryBlockMapperIterator(
                memory = this,
                first = first,
                mode = mode,
            ),
        )
        if (mode.write) {
            if (mode.append) {
                return channel.append()
            } else if (mode.truncate) {
                return channel.truncate()
            }
        }
        return channel
    }

    fun newByteChannel(
        mode: MemoryOpenOptions = MemoryOpenOptions.WRITE,
        lock: MemoryFileLock? = null,
        prev: Block = Block.InvalidBlock,
        bodySize: ByteSize = ByteSize.InvalidSize
    ): FragmentedReadWriteBuffer {
        require(mode.write)

        val maxBlockSize = if (bodySize.isValid()) {
            bodySize + headerBytes
        } else {
            this.maxBlockSize
        }
        val minBlockSize = headerBytes
        val reserveBySize: BitmapEntry = bitmap.allocate(
            minBlockSize = minBlockSize,
            maxBlockSize = maxBlockSize,
            prev = prev,
        )
//        if (prev.isValid()) {
//            if (prev.end == reserveBySize.start) {
//                TODO("Not yet implemented")
//            }
//        }
        assertTrue(reserveBySize.start != prev.start)
        val first = MemoryBlockReadWriteMapper.newBlock(
            memory = this,
            offset = reserveBySize,
            bodySize = ByteSize(value = reserveBySize.size - headerBytes.size),
        )
        return FragmentedReadWriteBuffer(
            lock = lock,
            data = MemoryBlockMapperIterator(
                memory = this,
                first = first,
                mode = mode,
            ),
        )
    }

    fun release(offset: BlockStart) {
        val first = MemoryBlockReadWriteMapper.existingBlock(memory = this, offset = offset)
        release(block = first)
    }

    fun release(block: Block) {
        bitmap.release(block = block)
    }

    companion object {
        operator fun invoke(
            memory: MemorySegment,
            bitmap: BitmapRegistry,
            maxBlockByteSize: ByteSize,
        ): MemoryLedger {
            if (memory.byteSize() <= MemoryReadWriteBuffer.MaxUnsignedIntInclusive) {
                return IntMemoryLedger(
                    memory = memory,
                    bitmap = bitmap,
                    maxBlockByteSize = maxBlockByteSize,
                )
            } else {
                return LongMemoryLedger(
                    memory = memory,
                    bitmap = bitmap,
                    maxBlockByteSize = maxBlockByteSize,
                )
            }
        }
    }
}