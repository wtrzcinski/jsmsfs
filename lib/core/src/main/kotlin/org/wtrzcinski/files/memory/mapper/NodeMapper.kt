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

package org.wtrzcinski.files.memory.mapper

import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.schema.MapperSchema
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.ModeState
import org.wtrzcinski.files.memory.node.NodeType
import org.wtrzcinski.files.memory.util.Check

@Suppress("unused")
class NodeMapper(
    mode: Mode,
    private val schema: MapperSchema,
    private val buffer: MemoryReadWriteBuffer,
) : BlockBodyMapper, ModeState(mode) {

    fun readType(): NodeType {
        checkIsReadable()
        checkPosition(schema.offsetRange("type"))

        return NodeType.entries[buffer.readInt()]
    }

    fun writeType(type: NodeType) {
        checkIsWritable()
        checkPosition(schema.offsetRange("type"))

        buffer.writeInt(type.ordinal)
    }

    fun readDataOffset(): BlockOffset {
        checkIsReadable()
        checkPosition(schema.offsetRange("data"))

        return checkNotNull(buffer.readOffset())
    }

    fun writeDataOffset(offset: BlockOffset) {
        checkIsWritable()
        checkPosition(schema.offsetRange("data"))

        buffer.writeOffset(offset)
    }

    fun readAttrsOffset(): BlockOffset {
        checkIsReadable()
        checkPosition(schema.offsetRange("attrs"))

        return checkNotNull(buffer.readOffset())
    }

    fun writeAttrsOffset(offset: BlockOffset) {
        checkIsWritable()
        checkPosition(schema.offsetRange("attrs"))

        buffer.writeOffset(offset)
    }

    fun readNameOffset(): BlockOffset {
        checkIsReadable()
        checkPosition(schema.offsetRange("name"))

        return checkNotNull(buffer.readOffset())
    }

    fun writeNameOffset(offset: BlockOffset) {
        checkIsWritable()
        checkPosition(schema.offsetRange("name"))

        buffer.writeOffset(offset)
    }

    override fun flip(): BlockOffset {
        checkIsWritable()
        checkPosition(schema.offsetRange)

        if (tryFlip()) {
            return buffer.flip()
        } else {
            throw MemoryIllegalStateException()
        }
    }

    private fun checkPosition(range: ClosedRange<BlockOffset>) {
        Check.isTrue { BlockOffset(buffer.position()) in range }
    }

}