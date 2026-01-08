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

import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.schema.ValueHandler
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.InvalidRef
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.intByteSize
import org.wtrzcinski.files.memory.util.Check

class Int32AddressHandler : ValueHandler<BlockAddress?> {

    override val byteSize: ByteSize get() = intByteSize

    override fun read(buffer: MemoryReadWriteBuffer): BlockAddress? {
        val value = buffer.readUnsignedInt() ?: return null
        Check.isTrue { value >= 0 }
        return BlockAddress.Companion(value)
    }

    override fun write(buffer: MemoryReadWriteBuffer, value: BlockAddress?) {
        if (value == null || !value.isValid()) {
            buffer.writeInt(InvalidRef.toInt())
        } else {
            Check.isTrue { value.start >= 0 }
            buffer.writeUnsignedInt(value.start)
        }
    }

}