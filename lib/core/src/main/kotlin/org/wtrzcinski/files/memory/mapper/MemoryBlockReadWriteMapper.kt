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

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.BufferAllocator
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class MemoryBlockReadWriteMapper(
    val memory: BufferAllocator,
    private val offset: BlockOffset,
    private val header: MemoryHeaderMapper,
    val body: MemoryReadWriteBuffer,
) : Block {

    override val start: Long get() = offset.start

    override val size: Long get() = (readBodySize() + header.size).size

    fun readBodySize(): ByteSize {
        return header.readBodySize
    }

    fun readNextOffset(): BlockOffset? {
        return header.readNextOffset
    }

    fun writeBodySizeAndTruncate(newValue: ByteSize) {
        val byteBuffer = header.bodySizeBuffer
        byteBuffer.clear()
        val prevValue = byteBuffer.readSize()
        if (prevValue != newValue) {
            val divide = this.div(newSize = newValue + header.size)

            memory.releaseOne(block = divide.second)

            byteBuffer.clear()
            byteBuffer.writeSize(value = newValue)

            body.truncate(newValue.size)
        }
    }

    fun writeNextOffsetAndRelease(newValue: BlockOffset) {
        val byteBuffer = header.nextOffsetBuffer
        byteBuffer.clear()
        val prevValue = byteBuffer.readOffset()
        byteBuffer.clear()
        if (prevValue != newValue) {
            if (prevValue != null && prevValue.isValid()) {
                memory.releaseAll(offset = prevValue)
            }

            byteBuffer.clear()
            byteBuffer.writeOffset(value = newValue)
        }
    }

    override fun toString(): String {
        val next = readNextOffset()?.start
        val headerSize = header.size
        val bodySize = readBodySize()
        return "${javaClass.simpleName}(start=$start, end=$endExclusive, size=$size, headerSize=$headerSize, bodySize=$bodySize, next=$next)"
    }
}