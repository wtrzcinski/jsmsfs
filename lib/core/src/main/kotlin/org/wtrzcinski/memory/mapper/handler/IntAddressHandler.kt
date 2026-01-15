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

package org.wtrzcinski.memory.mapper.handler

import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.InvalidRef

class IntAddressHandler(
    memoryByteSize: Long,
): SimpleVarHandler<BlockAddress> {

    private val byteCount: Int = UnsignedInt.roundToBytes(memoryByteSize)

    private val bitCount: Int = byteCount * 8

    override fun byteSize(): DefaultBlockSize = DefaultBlockSize(byteCount)

    override fun readNullable(buffer: MemoryReadWriteBuffer): BlockAddress? {
        val bytes = ByteArray(byteCount)
        buffer.read(bytes)
        val asLong = UnsignedInt.bytesToInt(byteCount, bytes) ?: return null
        if ((asLong + 1).shr(bitCount) == 1L) {
            return null
        }
        return BlockAddress(asLong)
    }

    override fun write(buffer: MemoryReadWriteBuffer, value: BlockAddress) {
        if (!value.isValid()) {
            val longToBytes = UnsignedInt.intToBytes(this.byteCount, InvalidRef)
            buffer.write(longToBytes)
        } else {
            val longToBytes = UnsignedInt.intToBytes(this.byteCount, value.start)
            buffer.write(longToBytes)
        }

    }
}