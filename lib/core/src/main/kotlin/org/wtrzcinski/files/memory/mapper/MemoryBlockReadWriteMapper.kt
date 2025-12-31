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

package org.wtrzcinski.files.memory.mapper

import org.wtrzcinski.files.memory.MemorySegmentLedger
import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.chunk.ChunkReadWriteBuffer
import org.wtrzcinski.files.memory.util.HistoricalLog
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class MemoryBlockReadWriteMapper(
    val memory: MemorySegmentLedger,
    override val start: Long,
    private val header: MemoryHeaderMapper,
    val body: ChunkReadWriteBuffer,
) : Block {

    companion object {
        fun newBlock(memory: MemorySegmentLedger, offset: BlockStart, bodySize: ByteSize): MemoryBlockReadWriteMapper {
            val header = MemoryHeaderMapper.newHeader(memory = memory, offset = offset, bodySize = bodySize)
            val body = memory.directBuffer(start = offset + header.size, size = header.readBodySize)
            return MemoryBlockReadWriteMapper(
                memory = memory,
                start = offset.start,
                header = header,
                body = body,
            )
        }

        fun existingBlock(memory: MemorySegmentLedger, offset: BlockStart): MemoryBlockReadWriteMapper {
            val header = MemoryHeaderMapper.existingHeader(memory = memory, offset = offset)
            val bodySize = header.readBodySize
            val body = memory.directBuffer(start = offset + header.size, size = bodySize)
            return MemoryBlockReadWriteMapper(
                memory = memory,
                start = offset.start,
                header = header,
                body = body,
            )
        }
    }

    override val size: Long
        get() {
            return (readBodySize() + memory.headerBytes).size
        }

    fun readBodySize(): ByteSize {
        return header.readBodySize
    }

    fun readNextOffset(): BlockStart? {
        return header.readNextOffset
    }

    fun writeBodySizeAndTruncate(newValue: ByteSize) {
        val byteBuffer = header.bodySizeBuffer
        byteBuffer.clear()
        val prevValue = byteBuffer.readSize()
        if (prevValue != newValue) {
            val divide = this.div(newSize = newValue + memory.headerBytes)

            HistoricalLog.debug(this) { "writeBodySizeAndTruncate ${divide.first}, ${divide.second}" }

            memory.release(block = divide.second)

            byteBuffer.clear()
            byteBuffer.writeSize(value = newValue)

            body.limit(newValue)
        }
    }

    fun writeNextOffsetAndRelease(newValue: BlockStart) {
        val byteBuffer = header.nextOffsetBuffer
        byteBuffer.clear()
        val prevValue = byteBuffer.readOffset()
        byteBuffer.clear()
        if (prevValue != newValue) {
            if (prevValue != null && prevValue.isValid()) {
                memory.release(offset = prevValue)
            }

            byteBuffer.clear()
            byteBuffer.writeOffset(value = newValue)
        }
    }

    override fun toString(): String {
        val next = readNextOffset()?.start
        val headerSize = memory.headerBytes
        val bodySize = readBodySize()
        return "${javaClass.simpleName}(start=$start, end=$end, size=$size, headerSize=$headerSize, bodySize=$bodySize, next=$next)"
    }
}