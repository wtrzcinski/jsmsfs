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
import org.wtrzcinski.memory.mapper.NodeType
import org.wtrzcinski.memory.mapper.handler.EnumVarHandler

class NodeStructSchema(private val delegate: StructSchema) {
    companion object {
        private val type = PropertyDefinition<NodeType>(type = NodeType::class, name = "type")

        private val linkCount = PropertyDefinition<DefaultBlockSize>(type = DefaultBlockSize::class, name = "linkCount")

        private val data = PropertyDefinition<BlockAddress>(type = BlockAddress::class, name = "data")

        private val attrs = PropertyDefinition<BlockAddress>(type = BlockAddress::class, name = "attrs")

        private val name = PropertyDefinition<BlockAddress>(type = BlockAddress::class, name = "name")

        fun nodeSchema(schemas: SchemaRegistry): NodeStructSchema = NodeStructSchema(
            StructSchema.builder()
                .property(def = type, handler = EnumVarHandler(entries = NodeType.entries))
                .property(def = linkCount, handler = schemas.listSizeHandler)
                .property(def = data, handler = schemas.refHandler)
                .property(def = attrs, handler = schemas.refHandler)
                .property(def = name, handler = schemas.refHandler)
                .build()
        )
    }

    val type = delegate.property(Companion.type)

    val linkCount = delegate.property(Companion.linkCount)

    val dataRef = delegate.property(data)

    val attrsRef = delegate.property(attrs)

    val nameRef = delegate.property(name)

    val addressRange = delegate.addressRange

    fun bodySize() = delegate.bodySize()
}