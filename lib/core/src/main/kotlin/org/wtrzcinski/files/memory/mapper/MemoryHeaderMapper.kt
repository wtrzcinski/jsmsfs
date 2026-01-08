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

package org.wtrzcinski.files.memory.mapper

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.mode.OpenMode

data class MemoryHeaderMapper(
    val offset: BlockAddress,
    val bodySizeBuffer: MemoryReadWriteBuffer,
    val nextOffsetBuffer: MemoryReadWriteBuffer,
) : Mapper, Block {

    override val start: Long
        get() {
            return offset.start
        }

    override val size: Long
        get() {
            return bodySizeBuffer.size() + nextOffsetBuffer.size()
        }

    val readBodySize: ByteSize
        get() {
            val byteBuffer = bodySizeBuffer
            byteBuffer.clear()
            return byteBuffer.readSize()
        }

    val readNextOffset: BlockAddress?
        get() {
            val byteBuffer = nextOffsetBuffer
            byteBuffer.clear()
            val nextRef = byteBuffer.readOffset()
            if (nextRef != null && nextRef.isValid()) {
                return nextRef
            }
            return null
        }

    override fun close() {
    }

    override fun flip(mode: OpenMode): BlockAddress {
        return offset
    }
}