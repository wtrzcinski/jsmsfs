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

import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.ModeMonitor
import org.wtrzcinski.files.memory.mode.OpenMode
import org.wtrzcinski.files.memory.schema.StructSchema
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.intByteSize
import org.wtrzcinski.files.memory.util.Check
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
@Suppress("unused")
class NodeMapper(
    mode: Mode,
    private val mappers: MemoryMapperRegistry,
    private val schema: StructSchema,
    private val buffer: MemoryReadWriteBuffer,
    private var ref: BlockAddress? = null,
) : BlockBodyMapper, ModeMonitor(mode) {

    fun ref(): BlockAddress {
        return checkNotNull(ref)
    }

    fun readType(): NodeType {
        synchronized(buffer) {
            throwIfNotReadable()
            val position = schema.offsetRange("type")
            setPosition(position)

            val readInt = buffer.readInt()
            return NodeType.entries[readInt]
        }
    }

    fun writeType(type: NodeType) {
        synchronized(buffer) {
            throwIfNotWritable()
            checkPosition(schema.offsetRange("type"))

            buffer.writeInt(type.ordinal)
        }
    }

    fun readLinkCount(): Int {
        synchronized(buffer) {
            throwIfNotReadable()
            setPosition(schema.offsetRange("linkCount"))
            return buffer.readInt()
        }
    }

    fun writeLinkCount(linkCount: Int) {
        synchronized(buffer) {
            throwIfNotWritable()
            checkPosition(schema.offsetRange("linkCount"))
            buffer.writeInt(linkCount)
        }
    }

    fun readDataRef(): BlockAddress? {
        synchronized(buffer) {
            throwIfNotReadable()
            setPosition(schema.offsetRange("data"))

            return buffer.readOffset()
        }
    }

    fun writeDataRef(ref: BlockAddress) {
        synchronized(buffer) {
            throwIfNotWritable()
            setPosition(schema.offsetRange("data"))

            buffer.writeOffset(ref)
        }
    }

    fun readChildrenRefs(): Sequence<BlockAddress> {
        synchronized(buffer) {
            val dataRef = readDataRef() ?: return sequenceOf()
            return readChildrenRefs(dataRef)
        }
    }

    fun readChildrenRefs(ref: BlockAddress): Sequence<BlockAddress> {
        synchronized(buffer) {
            val lock = MemoryFileLock.unlocked(Mode.read())
            val channel = mappers.ledger.existingChannel(name = "", ref = ref, lock = lock)
            channel.use {
                return it.readRefs()
            }
        }
    }

    fun findChildren(): Sequence<NodeMapper> {
        synchronized(buffer) {
            val readChildrenRefs = readChildrenRefs()
            return readChildrenRefs.map {
                this.mappers.readNode(it)
            }
        }
    }

    fun findChildByName(name: String): NodeMapper? {
        synchronized(buffer) {
            val findChildIds = readChildrenRefs()
            return findChildByName(findChildIds, name)
        }
    }

    private fun findChildByName(refs: Sequence<BlockAddress>, name: String): NodeMapper? {
        synchronized(buffer) {
            for (id in refs) {
                val readNode = this.mappers.readNode(id)
                if (readNode.readName() == name) {
                    return readNode
                }
            }
            return null
        }
    }

    fun hasChildren(): Boolean {
        synchronized(buffer) {
            val readChildIds = readChildrenRefs()
            return readChildIds.iterator().hasNext()
        }
    }

    fun addChild(nodeMapper: NodeMapper) {
        synchronized(buffer) {
            val children: Sequence<BlockAddress> = readChildrenRefs() + nodeMapper.ref()
            upsertChildren(prevDataRef = readDataRef(), children = children)
        }
    }

    fun removeChild(child: NodeMapper) {
        synchronized(buffer) {
            val name = child.readName()
            val childIds = readChildrenRefs()
            val findChildByName = findChildByName(childIds, name)
            require(findChildByName != null)

            val children = mutableListOf<BlockAddress>()
            children.addAll(childIds)
            children.remove(findChildByName.ref())
            upsertChildren(prevDataRef = readDataRef(), children = children.asSequence())
        }
    }

    private fun upsertChildren(prevDataRef: BlockAddress?, children: Sequence<BlockAddress>) {
        synchronized(buffer) {
            if (prevDataRef != null && prevDataRef.isValid()) {
                mappers.ledger.releaseAll(ref = prevDataRef)
            }

            val childrenCount = children.count()
            val newDataRef: BlockAddress = if (childrenCount > 0) {
                val exactBodySize = intByteSize + (mappers.ledger.addressSchema.handler.byteSize * childrenCount)
                val newDataByteChannel = mappers.ledger.allocateChannel(exactBodySize = exactBodySize)
                newDataByteChannel.use {
                    it.writeOffsets(children)
                }
                newDataByteChannel.address()
            } else {
                BlockAddress.InvalidOffset
            }
            writeDataRef(newDataRef)
        }
    }

    fun readAttrsRef(): BlockAddress {
        synchronized(buffer) {
            throwIfNotReadable()
            setPosition(schema.offsetRange("attrs"))

            return checkNotNull(buffer.readOffset())
        }
    }

    fun writeAttrsRef(ref: BlockAddress) {
        synchronized(buffer) {
            throwIfNotWritable()
            checkPosition(schema.offsetRange("attrs"))

            buffer.writeOffset(ref)
        }
    }

    fun readAttrs(): AttrsMapper {
        synchronized(buffer) {
            return mappers.readAttrs(readAttrsRef())
        }
    }

    fun readNameRef(): BlockAddress {
        synchronized(buffer) {
            throwIfNotReadable()
            setPosition(schema.offsetRange("name"))

            return checkNotNull(buffer.readOffset())
        }
    }

    fun writeNameRef(offset: BlockAddress) {
        synchronized(buffer) {
            throwIfNotWritable()
            setPosition(schema.offsetRange("name"))

            buffer.writeOffset(offset)
        }
    }

    fun readName(): String {
        synchronized(buffer) {
            val address = readNameRef()
            val mode1 = Mode.read()
            val lock = MemoryFileLock.unlocked(mode1)
            val channel = mappers.ledger.existingChannel(name = "", ref = address, lock = lock)
            channel.use {
                return it.readString()
            }
        }
    }

    override fun flip(mode: OpenMode): BlockAddress {
        synchronized(buffer) {
            throwIfNotWritable()
            checkPosition(schema.offsetRange)

            if (tryFlip()) {
                this.ref = buffer.flip()
                return checkNotNull(this.ref)
            } else {
                throw MemoryIllegalStateException()
            }
        }
    }

    private fun checkPosition(range: ClosedRange<BlockAddress>) {
        Check.isTrue { BlockAddress(buffer.position()) in range }
    }

    private fun setPosition(range: ClosedRange<BlockAddress>) {
        require(range.start == range.endInclusive)
        buffer.position(range.start.start)
    }

}