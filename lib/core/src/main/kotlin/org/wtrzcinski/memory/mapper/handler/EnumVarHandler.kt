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

class EnumVarHandler<T>(
    val entries: List<T>,
) : SimpleVarHandler<T> {
    override fun byteSize(): DefaultBlockSize {
        return DefaultBlockSize(Byte.SIZE_BYTES)
    }

    override fun read(buffer: MemoryReadWriteBuffer): T {
        val toInt = buffer.readByte().toInt()
        return entries[toInt]
    }

    override fun write(buffer: MemoryReadWriteBuffer, value: T) {
        val indexOf = entries.indexOf(value)
        val toByte = indexOf.toByte()
        check(toByte >= 0)
        buffer.writeByte(toByte)
    }
}