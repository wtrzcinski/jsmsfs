/**
 * Copyright 2025 Wojciech Trzciński
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

import org.wtrzcinski.files.memory.MemorySegmentLedger
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.buffer.channel.FragmentedReadWriteBuffer
import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.intByteSize
import org.wtrzcinski.files.memory.mode.AbstractCloseable
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.node.NodeType
import org.wtrzcinski.files.memory.util.Check.isTrue

class NodeMapper(
    name: String,
    memory: MemorySegmentLedger,
    mode: Mode,
) : BlockBodyMapper, AbstractCloseable(mode) {

    private val dataPosition: Long = intByteSize.size
    private val attrPosition: Long = intByteSize.size + memory.offsetBytes.toLong()
    private val namePosition: Long = intByteSize.size + (memory.offsetBytes.toLong() * 2)
    private val bodySize = intByteSize + (memory.offsetBytes * 3)

    private val buffer: FragmentedReadWriteBuffer = memory.newBuffer(name = name, bodyAlignment = bodySize)

    fun writeType(type: NodeType) {
        checkIsWritable()
        isTrue { buffer.position() == 0L }

        buffer.writeInt(type.ordinal)
    }

    fun writeDataOffset(offset: BlockStart) {
        isTrue { buffer.position() == dataPosition }

        buffer.writeOffset(offset)
    }

    fun writeAttrOffset(offset: BlockStart) {
        isTrue { buffer.position() == attrPosition }

        buffer.writeOffset(offset)
    }

    fun writeNameOffset(offset: BlockStart) {
        isTrue { buffer.position() == namePosition }

        buffer.writeOffset(offset)
    }

    override fun flip(): BlockStart {
        if (tryFlip()) {
            isTrue { buffer.position() == bodySize.size }

            return buffer.flip()
        } else {
            throw MemoryIllegalStateException()
        }
    }
}