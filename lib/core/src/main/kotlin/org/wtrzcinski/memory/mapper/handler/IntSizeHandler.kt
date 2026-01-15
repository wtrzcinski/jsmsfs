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

import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.buffer.MemoryReadWriteBuffer

class IntSizeHandler(
    memoryByteSize: Long,
) : SimpleVarHandler<DefaultBlockSize> {

    private val byteCount: Int = UnsignedInt.roundToBytes(memoryByteSize)

    override fun byteSize(): DefaultBlockSize = DefaultBlockSize(byteCount)

    override fun read(buffer: MemoryReadWriteBuffer): DefaultBlockSize {
        val bytes = ByteArray(byteCount)
        buffer.read(bytes)
        val asLong = UnsignedInt.bytesToInt(byteCount = byteCount, bytes = bytes) ?: return DefaultBlockSize.InvalidSize
        return DefaultBlockSize(value = asLong)
    }

    override fun write(buffer: MemoryReadWriteBuffer, value: DefaultBlockSize) {
        val longToBytes = UnsignedInt.intToBytes(byteCount = this.byteCount, value = value.size)
        buffer.write(value = longToBytes)
    }

}