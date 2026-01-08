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

package org.wtrzcinski.files.memory.schema.handler

import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.schema.ValueHandler

class IntSizeSchemaHandler(
    memoryByteSize: Long,
) : ValueHandler<ByteSize> {

    private val byteCount: Int = FloatingInt.roundToBytes(memoryByteSize)

    override val byteSize: ByteSize = ByteSize(byteCount)

    override fun read(buffer: MemoryReadWriteBuffer): ByteSize {
        val bytes = ByteArray(byteCount)
        buffer.read(bytes)
        val asLong = FloatingInt.bytesToInt(bitCount = byteCount, byteArray = bytes) ?: return ByteSize.InvalidSize
        return ByteSize(value = asLong)
    }

    override fun write(buffer: MemoryReadWriteBuffer, value: ByteSize) {
        val longToBytes = FloatingInt.intToBytes(bitCount = this.byteCount, value = value.size)
        buffer.write(value = longToBytes)
    }

}