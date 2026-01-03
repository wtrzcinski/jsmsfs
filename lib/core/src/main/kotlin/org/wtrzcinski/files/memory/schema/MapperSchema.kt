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

package org.wtrzcinski.files.memory.schema

import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.address.DefaultBlockOffset

class MapperSchema(
    fields: List<MapperField>,
) : ClosedRange<ByteSize> {

    companion object {

        fun builder(): Builder {
            return Builder()
        }

    }

    class Builder {

        val fields = mutableListOf<MapperField>()

        fun field(name: String, size: ByteSize) = apply {
            fields.add(MapperField(name = name, size = size))
        }

        fun field(name: String, minSize: ByteSize, maxSize: ByteSize) = apply {
            this.fields.add(MapperField(name = name, range = minSize.rangeTo(maxSize)))
        }

        fun build(): MapperSchema {
            return MapperSchema(this)
        }

    }

    constructor(builder: Builder) : this(builder.fields)

    private val sizeRange: ClosedRange<ByteSize>

    private val offsetRanges: Map<String, ClosedRange<BlockOffset>>

    init {
        val offsetRanges1 = mutableMapOf<String, ClosedRange<BlockOffset>>()
        var start = ByteSize.EmptySize
        var endInclusive = ByteSize.EmptySize
        for (field in fields) {
            offsetRanges1[field.name] = BlockOffset(start.size)..BlockOffset(endInclusive.size)
            start += field.start
            endInclusive += field.endInclusive
        }
        sizeRange = start..endInclusive
        this.offsetRanges = offsetRanges1
    }

    override val start: ByteSize get() = sizeRange.start

    override val endInclusive: ByteSize get() = sizeRange.endInclusive

    val offsetRange: ClosedRange<BlockOffset> get() {
        return DefaultBlockOffset(start.size)..DefaultBlockOffset(endInclusive.size)
    }

    fun bodyAlignment(): ByteSize {
        return sizeRange.endInclusive
    }

    fun bodySize(): ByteSize {
        check(sizeRange.start == sizeRange.endInclusive)

        return sizeRange.endInclusive
    }

    fun offsetRange(name: String): ClosedRange<BlockOffset> {
        return checkNotNull(offsetRanges[name])
    }

}