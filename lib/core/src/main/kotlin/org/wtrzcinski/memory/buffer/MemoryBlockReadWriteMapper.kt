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

package org.wtrzcinski.memory.buffer

import org.wtrzcinski.memory.address.Block
import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.mapper.MemoryHeaderMapper
import org.wtrzcinski.memory.mapper.schema.SchemaRegistry
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class MemoryBlockReadWriteMapper(
    val memory: BufferAllocator,
    val schemas: SchemaRegistry,
    address: BlockAddress,
    private val header: MemoryHeaderMapper,
    val body: AbstractMemoryReadWriteBuffer,
) : Block {

    override val start: Long = address.start

    override val size: Long get() = (readBodySize() + header.size).size

    fun readBodySize(): DefaultBlockSize {
        return header.readBodySize
    }

    fun readNextAddress(): BlockAddress? {
        return header.readNextOffset
    }

    fun writeBodySizeAndRelease(newValue: DefaultBlockSize) {
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

    fun writeNextAddressAndRelease(newValue: BlockAddress) {
        val byteBuffer = header.nextOffsetBuffer
        byteBuffer.clear()
        val prevValue = byteBuffer.readRef()
        byteBuffer.clear()
        if (prevValue != newValue) {
            if (prevValue != null && prevValue.isValid()) {
                memory.releaseAll(ref = prevValue)
            }

            byteBuffer.clear()
            byteBuffer.writeRef(value = newValue)
        }
    }

    override fun toString(): String {
        val next = readNextAddress()?.start
        val headerSize = header.size
        val bodySize = readBodySize()
        return "${javaClass.simpleName}(start=$start, end=$endExclusive, size=$size, headerSize=$headerSize, bodySize=$bodySize, next=$next)"
    }
}