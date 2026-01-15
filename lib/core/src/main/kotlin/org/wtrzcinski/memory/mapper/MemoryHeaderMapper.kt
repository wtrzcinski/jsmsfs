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

package org.wtrzcinski.memory.mapper

import org.wtrzcinski.memory.address.Block
import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.memory.mode.OpenMode

data class MemoryHeaderMapper(
    val address: BlockAddress,
    val bodySizeBuffer: MemoryReadWriteBuffer,
    val nextOffsetBuffer: MemoryReadWriteBuffer,
) : Mapper, Block {

    override val start: Long
        get() {
            return address.start
        }

    override val size: Long
        get() {
            return bodySizeBuffer.size() + nextOffsetBuffer.size()
        }

    val readBodySize: DefaultBlockSize
        get() {
            val byteBuffer = bodySizeBuffer
            byteBuffer.clear()
            return byteBuffer.readSize()
        }

    val readNextOffset: BlockAddress?
        get() {
            val byteBuffer = nextOffsetBuffer
            byteBuffer.clear()
            val nextRef = byteBuffer.readRef()
            if (nextRef != null && nextRef.isValid()) {
                return nextRef
            }
            return null
        }

    override fun close() {
    }

    override fun flip(mode: OpenMode): BlockAddress {
        return address
    }
}