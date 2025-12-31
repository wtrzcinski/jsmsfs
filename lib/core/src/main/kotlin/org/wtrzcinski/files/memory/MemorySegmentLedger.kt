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
import org.wtrzcinski.files.memory.buffer.channel.FragmentedReadWriteBuffer
import org.wtrzcinski.files.memory.buffer.chunk.ChunkReadWriteBuffer
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.mapper.MemoryBlockIterator
import org.wtrzcinski.files.memory.mapper.MemoryBlockReadWriteMapper
import org.wtrzcinski.files.memory.provider.MemoryFileOpenOptions
import org.wtrzcinski.files.memory.util.Check
import java.lang.foreign.MemorySegment
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
abstract class MemorySegmentLedger(
    val name: String = "ledger",
    val memory: MemorySegment,
    val bitmap: BitmapRegistry,
    val maxBlockSize: ByteSize,
) {

    abstract val sizeBytes: ByteSize

    abstract val offsetBytes: ByteSize

    val headerBytes: ByteSize get() = sizeBytes + offsetBytes

    abstract fun directBuffer(start: BlockStart, size: ByteSize): ChunkReadWriteBuffer

    fun existingBuffer(
        name: String = "",
        offset: BlockStart,
        mode: MemoryFileOpenOptions = MemoryFileOpenOptions.READ,
        lock: MemoryFileLock? = null,
    ): FragmentedReadWriteBuffer {
        Check.isTrue { offset.isValid() }

        val first = MemoryBlockReadWriteMapper.existingBlock(memory = this, offset = offset)
        val channel = FragmentedReadWriteBuffer(
            lock = lock,
            data = MemoryBlockIterator(
                name = name,
                memory = this,
                first = first,
                mode = mode,
            ),
        )

        if (mode.readWrite) {
            if (mode.append) {
                return channel.append()
            } else if (mode.truncate) {
                return channel.truncate()
            }
        }

        return channel
    }

    fun newBuffer(
        name: String = "",
        lock: MemoryFileLock? = null,
        prev: Block = Block.InvalidBlock,
        bodyAlignment: ByteSize = ByteSize.InvalidSize,
        capacity: ByteSize = ByteSize.InvalidSize,
    ): FragmentedReadWriteBuffer {

        val maxBlockSize = if (bodyAlignment.isValid()) {
            bodyAlignment + headerBytes
        } else {
            this.maxBlockSize
        }

        val minBlockSize = headerBytes

        val reserveBySize: BitmapEntry = bitmap.allocate(
            name = name,
            minBlockSize = minBlockSize,
            maxBlockSize = maxBlockSize,
            prev = prev,
        )

//        if (prev.isValid()) {
//            if (prev.end == reserveBySize.start) {
//                TODO("Not yet implemented")
//            }
//        }

        Check.isTrue { reserveBySize.start != prev.start }

        val first = MemoryBlockReadWriteMapper.newBlock(
            memory = this,
            offset = reserveBySize,
            bodySize = ByteSize(value = reserveBySize.size - headerBytes.size),
        )
        val channel = FragmentedReadWriteBuffer(
            lock = lock,
            data = MemoryBlockIterator(
                name = name,
                memory = this,
                first = first,
                capacity = capacity,
                mode = MemoryFileOpenOptions.WRITE_TRUNCATE,
            ),
        )
        return channel
    }

    fun release(offset: BlockStart) {
        val existingBuffer = existingBuffer(offset = offset, mode = MemoryFileOpenOptions.WRITE_TRUNCATE)
        existingBuffer.skipRemaining()
        existingBuffer.close()
        existingBuffer.release()
    }

    fun release(block: Block) {
        bitmap.release(block = block)
    }

    companion object {
        operator fun invoke(
            memory: MemorySegment,
            bitmap: BitmapRegistry,
            maxBlockByteSize: ByteSize,
        ): MemorySegmentLedger {
            if (memory.byteSize() <= IntMemoryLedger.MaxUnsignedIntInclusive) {
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