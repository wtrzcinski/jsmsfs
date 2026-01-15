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

package org.wtrzcinski.memory.mapper.schema

import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler
import org.wtrzcinski.memory.mapper.handler.VarHandler
import org.wtrzcinski.memory.util.Check

data class PropertySchema<T: Any>(
    val def: PropertyDefinition<T>,
    val sizeRange: ClosedRange<DefaultBlockSize>,
    val addressRange: ClosedRange<BlockAddress> = BlockAddress.ZeroAddress.rangeTo(sizeRange.endInclusive.toAddress()),
    override val handler: VarHandler<T>,
) : ClosedRange<DefaultBlockSize> by sizeRange, ValueSchema<T> {

    constructor(
        def: PropertyDefinition<T>,
        handler: SimpleVarHandler<T>,
    ) : this(
        def = def,
        handler = handler,
        sizeRange = handler.byteSize().rangeTo(handler.byteSize()),
    )

    fun setPosition(buffer: MemoryReadWriteBuffer) {
        check(addressRange.start == addressRange.endInclusive)
        buffer.position(addressRange.start.start)
    }

    fun read(buffer: MemoryReadWriteBuffer): T {
        checkPosition(buffer)
        return handler.read(buffer)
    }

    fun readNullable(buffer: MemoryReadWriteBuffer): T? {
        checkPosition(buffer)
        return handler.readNullable(buffer)
    }

    fun write(buffer: MemoryReadWriteBuffer, value: T) {
        checkPosition(buffer)
        handler.write(buffer, value)
    }

    private fun checkPosition(buffer: MemoryReadWriteBuffer) {
        Check.isTrue { BlockAddress(buffer.position()) in addressRange }
    }
}