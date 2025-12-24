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

import org.wtrzcinski.files.memory.MemoryLedger
import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryByteBuffer

class MemoryHeaderMapper(
    val memory: MemoryLedger,
    val offset: BlockStart,
    val bodySizeBuffer: MemoryByteBuffer,
    val nextOffsetBuffer: MemoryByteBuffer,
) : DataMapper, Block {
    companion object {
        fun newHeader(memory: MemoryLedger, offset: BlockStart, bodySize: ByteSize): MemoryHeaderMapper {
            val bodySizeBuffer: MemoryByteBuffer = bodySizeBuffer(memory, offset)
            bodySizeBuffer.writeSize(value = bodySize)
            val nextOffsetBuffer: MemoryByteBuffer = nextOffsetBuffer(memory, offset)
            nextOffsetBuffer.writeOffset(value = BlockStart.InvalidAddress)
            return MemoryHeaderMapper(
                memory = memory,
                offset = offset,
                bodySizeBuffer = bodySizeBuffer,
                nextOffsetBuffer = nextOffsetBuffer
            )
        }

        fun existingHeader(memory: MemoryLedger, offset: BlockStart): MemoryHeaderMapper {
            val bodySizeBuffer: MemoryByteBuffer = bodySizeBuffer(memory, offset)
            val nextOffsetBuffer: MemoryByteBuffer = nextOffsetBuffer(memory, offset)
            return MemoryHeaderMapper(
                memory = memory,
                offset = offset,
                bodySizeBuffer = bodySizeBuffer,
                nextOffsetBuffer = nextOffsetBuffer
            )
        }

        private fun bodySizeBuffer(dataRegistry: MemoryLedger, offset: BlockStart): MemoryByteBuffer {
            return dataRegistry.directBuffer(
                start = offset,
                size = dataRegistry.sizeBytes,
            )
        }

        private fun nextOffsetBuffer(dataRegistry: MemoryLedger, offset: BlockStart): MemoryByteBuffer {
            return dataRegistry.directBuffer(
                start = offset + dataRegistry.sizeBytes,
                size = dataRegistry.offsetBytes,
            )
        }
    }

    override val start: Long
        get() {
            return offset.start
        }

    override val size: Long
        get() {
            return memory.headerBytes.size
        }

    fun readBodySize(): ByteSize {
        val byteBuffer = bodySizeBuffer
        byteBuffer.clear()
        return byteBuffer.readSize()
    }

    fun readNextOffset(): BlockStart? {
        val byteBuffer = nextOffsetBuffer
        byteBuffer.clear()
        val nextRef = byteBuffer.readOffset()
        if (nextRef != null && nextRef.isValid()) {
            return nextRef
        }
        return null
    }

    override fun flip(): BlockStart {
        return offset
    }
}