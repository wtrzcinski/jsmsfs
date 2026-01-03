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

package org.wtrzcinski.files.memory

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.address.DefaultBlock
import org.wtrzcinski.files.memory.allocator.Int32MemorySegmentLedger
import org.wtrzcinski.files.memory.allocator.Int64MemorySegmentLedger
import org.wtrzcinski.files.memory.bitmap.BitmapEntry
import org.wtrzcinski.files.memory.bitmap.BitmapRegistry
import org.wtrzcinski.files.memory.buffer.BufferAllocator
import org.wtrzcinski.files.memory.buffer.ContinuousReadWriteBuffer
import org.wtrzcinski.files.memory.buffer.FragmentedReadWriteBuffer
import org.wtrzcinski.files.memory.buffer.MemoryReadBuffer.Companion.MaxUnsignedIntInclusive
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.mapper.MemoryBlockIterator
import org.wtrzcinski.files.memory.mapper.MemoryBlockReadWriteMapper
import org.wtrzcinski.files.memory.mapper.MemoryHeaderMapper
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.OpenMode
import org.wtrzcinski.files.memory.mode.WriteMode
import org.wtrzcinski.files.memory.util.Check
import java.lang.foreign.MemorySegment
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
abstract class MemorySegmentLedger(
    val name: String = "ledger",
    val memory: MemorySegment,
    val bitmap: BitmapRegistry,
    val maxBlockSize: ByteSize,
    val sizeBytes: ByteSize,
    val offsetBytes: ByteSize,
) : BufferAllocator {

    val headerBytes: ByteSize get() = sizeBytes + offsetBytes

    init {
        Check.isTrue { maxBlockSize >= headerBytes }
    }

    abstract fun continuousBuffer(address: BlockOffset, start: BlockOffset, size: ByteSize): ContinuousReadWriteBuffer

    override fun existingBuffer(offset: BlockOffset): MemoryBlockReadWriteMapper {
        Check.isTrue { offset.isValid() }

//        header
        val bodySizeBuffer = bodySizeBuffer(offset)
        val nextOffsetBuffer = nextOffsetBuffer(offset)
        val header = MemoryHeaderMapper(
            offset = offset,
            bodySizeBuffer = bodySizeBuffer,
            nextOffsetBuffer = nextOffsetBuffer,
        )
//        body
        val body = continuousBuffer(
            address = offset,
            start = offset + header.size,
            size = header.readBodySize,
        )
        return MemoryBlockReadWriteMapper(
            memory = this,
            offset = offset,
            header = header,
            body = body,
        )
    }

    override fun existingChannel(offset: BlockOffset, mode: Mode, lock: MemoryFileLock?): MemoryReadWriteBuffer {
        val first = existingBuffer(offset)
        if (mode.open == OpenMode.ReadOnly) {
            if (first.readNextOffset() == null) {
                return first.body.onClose { lock?.release() }
            }
        }

        val channel = FragmentedReadWriteBuffer(
            data = MemoryBlockIterator(
                memory = this,
                first = first,
                mode = mode,
            ),
            close = { lock?.release() },
        )

        if (mode.open == OpenMode.ReadWrite) {
            if (mode.write == WriteMode.AppendToExisting) {
                return channel.append()
            } else if (mode.write == WriteMode.TruncateExisting) {
                return channel.truncate()
            }
        }

        return channel
    }

    override fun allocateBuffer(prev: Block, bodyAlignment: ByteSize, bodySize: ByteSize): MemoryBlockReadWriteMapper {
        val maxBlockSize: ByteSize
        val minBlockSize: ByteSize
        if (bodySize.isValid()) {
            minBlockSize = bodySize + this.headerBytes
            maxBlockSize = bodySize + this.headerBytes
        } else if (bodyAlignment.isValid()) {
            minBlockSize = this.headerBytes
            maxBlockSize = bodyAlignment + this.headerBytes
        } else {
            minBlockSize = this.headerBytes
            maxBlockSize = this.maxBlockSize
        }

        val reserved: DefaultBlock = bitmap.allocate(
            name = name,
            minBlockSize = minBlockSize,
            maxBlockSize = maxBlockSize,
            prev = BitmapEntry(prev),
        )

        if (bodySize.isValid()) {
            Check.isTrue { reserved.size == bodySize.size + this.headerBytes.size }
        }

//        header
        val bodySize = ByteSize(value = reserved.size - headerBytes.size)
        val header = MemoryHeaderMapper(
            offset = reserved,
            bodySizeBuffer = bodySizeBuffer(reserved).writeSize(value = bodySize),
            nextOffsetBuffer = nextOffsetBuffer(reserved).writeOffset(value = BlockOffset.InvalidOffset),
        )
//        body
        val body: MemoryReadWriteBuffer = continuousBuffer(
            address = reserved,
            start = header.offset + header.size,
            size = header.readBodySize,
        )
        return MemoryBlockReadWriteMapper(
            memory = this,
            offset = reserved,
            header = header,
            body = body,
        )
    }

    override fun allocateChannel(
        lock: MemoryFileLock?,
        bodyAlignment: ByteSize,
        bodySize: ByteSize,
    ): MemoryReadWriteBuffer {
        val first = allocateBuffer(prev = Block.InvalidBlock, bodyAlignment = bodyAlignment, bodySize = bodySize)
        if (bodySize.isValid()) {
            return first.body.onClose { lock?.release() }
        }
        return FragmentedReadWriteBuffer(
            data = MemoryBlockIterator(
                memory = this,
                first = first,
                capacity = bodySize,
                mode = Mode.createRead(),
            ),
            close = { lock?.release() },
        )
    }

    override fun releaseOne(block: Block) {
        bitmap.release(block = block)
    }

    override fun releaseAll(offset: BlockOffset) {
        val existingBuffer = existingChannel(offset = offset, mode = Mode.createRead())
        existingBuffer.skipRemaining()
        existingBuffer.close()
        existingBuffer.release()
    }

    private fun bodySizeBuffer(offset: BlockOffset): MemoryReadWriteBuffer {
        return continuousBuffer(address = offset, start = offset, size = sizeBytes)
    }

    private fun nextOffsetBuffer(offset: BlockOffset): MemoryReadWriteBuffer {
        return continuousBuffer(address = offset, start = offset + sizeBytes, size = offsetBytes)
    }

    companion object {
        operator fun invoke(
            memory: MemorySegment,
            bitmap: BitmapRegistry,
            maxBlockByteSize: ByteSize,
        ): MemorySegmentLedger {
            if (memory.byteSize() <= MaxUnsignedIntInclusive) {
                return Int32MemorySegmentLedger(
                    memory = memory,
                    bitmap = bitmap,
                    maxBlockByteSize = maxBlockByteSize,
                )
            } else {
                return Int64MemorySegmentLedger(
                    memory = memory,
                    bitmap = bitmap,
                    maxBlockByteSize = maxBlockByteSize,
                )
            }
        }
    }
}