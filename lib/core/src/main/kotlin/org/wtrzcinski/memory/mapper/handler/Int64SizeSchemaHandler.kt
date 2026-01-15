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
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.longByteSize

object Int64SizeSchemaHandler : SimpleVarHandler<DefaultBlockSize> {

    override fun byteSize(): DefaultBlockSize = longByteSize

    override fun read(buffer: MemoryReadWriteBuffer): DefaultBlockSize {
        val value = buffer.readLong()
        require(value >= 0)
        return DefaultBlockSize(value)
    }

    override fun write(buffer: MemoryReadWriteBuffer, value: DefaultBlockSize) {
        require(value.size >= 0)
        buffer.writeLong(value.size)
    }
}