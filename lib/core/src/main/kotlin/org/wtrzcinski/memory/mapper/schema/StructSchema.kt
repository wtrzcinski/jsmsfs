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
import org.wtrzcinski.memory.address.DefaultBlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler
import org.wtrzcinski.memory.mapper.handler.StringVarHandler

@Suppress("UNCHECKED_CAST")
open class StructSchema(
    fields: List<PropertySchema<*>> = listOf(),
) : ClosedRange<DefaultBlockSize>, Schema {

    companion object {
        fun builder(): Builder {
            return Builder()
        }
    }

    class Builder {

        val fields = mutableListOf<PropertySchema<*>>()

        fun <T: Any> property(def: PropertyDefinition<T>, handler: SimpleVarHandler<T>) = apply {
            fields.add(PropertySchema(def = def, handler = handler))
        }

        fun property(def: PropertyDefinition<String>, handler: StringVarHandler, maxBodySize: DefaultBlockSize) = apply {
            val headerHandler = handler.headerHandler()
            val headerSize = headerHandler.byteSize()
            this.fields.add(PropertySchema(
                def = def,
                sizeRange = headerSize.rangeTo(headerSize + maxBodySize),
                handler = handler,
            ))
        }

        fun build(): StructSchema {
            return StructSchema(this)
        }
    }

    constructor(builder: Builder) : this(builder.fields)

    private val sizeRange: ClosedRange<DefaultBlockSize>

    private val properties: Map<PropertyDefinition<*>, PropertySchema<*>>

    init {
        val properties1 = mutableMapOf<PropertyDefinition<*>, PropertySchema<*>>()
        var start = DefaultBlockSize.EmptySize
        var endInclusive = DefaultBlockSize.EmptySize
        for (field in fields) {
            val addressRange = BlockAddress(start.size)..BlockAddress(endInclusive.size)
            start += field.start
            endInclusive += field.endInclusive
            properties1[field.def] = field.copy(addressRange = addressRange)
        }
        sizeRange = start..endInclusive
        this.properties = properties1
    }

    override val start: DefaultBlockSize get() = sizeRange.start

    override val endInclusive: DefaultBlockSize get() = sizeRange.endInclusive

    open val addressRange: ClosedRange<BlockAddress> get() {
        return DefaultBlockAddress(start.size)..DefaultBlockAddress(endInclusive.size)
    }

    fun maxBodySize(): DefaultBlockSize {
        return sizeRange.endInclusive
    }

    open fun bodySize(): DefaultBlockSize {
        check(sizeRange.start == sizeRange.endInclusive)

        return sizeRange.endInclusive
    }

    fun <T: Any> property(name: PropertyDefinition<T>): PropertySchema<T> {
        return requireNotNull(properties[name]) as PropertySchema<T>
    }
}