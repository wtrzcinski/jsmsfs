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

package org.wtrzcinski.memory.mapper

import org.wtrzcinski.memory.MemorySegmentContext
import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.memory.lock.MemoryFileLock
import org.wtrzcinski.memory.mapper.schema.NodeStructSchema
import org.wtrzcinski.memory.mode.Mode
import org.wtrzcinski.memory.mode.ModeMonitor
import org.wtrzcinski.memory.mode.OpenMode
import org.wtrzcinski.memory.util.Check
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
@Suppress("unused")
class NodeMapper(
    mode: Mode,
    private val context: MemorySegmentContext,
    private val schema: NodeStructSchema,
    private val buffer: MemoryReadWriteBuffer,
    private var ref: BlockAddress? = null,
) : BlockBodyMapper, ModeMonitor(mode) {

    fun ref(): BlockAddress {
        return checkNotNull(ref)
    }

    fun readType(): NodeType {
        throwIfNotReadable()
        val property = schema.type
        property.setPosition(buffer)
        val handler = property.handler
        return handler.read(buffer)
    }

    fun writeType(type: NodeType) {
        throwIfNotWritable()
        require(type == NodeType.Regular || type == NodeType.Directory || type == NodeType.SymbolicLink)
        val property = schema.type
        property.write(buffer, type)
    }

    fun readLinkCount(): DefaultBlockSize {
        throwIfNotReadable()
        val property = schema.linkCount
        property.setPosition(buffer)
        return property.read(buffer)
    }

    fun writeLinkCount(linkCount: DefaultBlockSize) {
        throwIfNotWritable()
        val property = schema.linkCount
        property.write(buffer, linkCount)
    }

    fun readDataRef(): BlockAddress? {
        throwIfNotReadable()
        val property = schema.dataRef
        property.setPosition(buffer)
        return property.readNullable(buffer)
    }

    fun writeDataRef(ref: BlockAddress) {
        throwIfNotWritable()
        val property = schema.dataRef
        property.setPosition(buffer)
        property.write(buffer, ref)
    }

    fun findChildren(): Sequence<NodeMapper> {
        synchronized(buffer) {
            val readChildrenRefs = readChildrenRefs()
            return readChildrenRefs.map {
                context.mappers.readNode(it)
            }
        }
    }

    fun findChildByName(name: String): NodeMapper? {
        synchronized(buffer) {
            val findChildIds = readChildrenRefs()
            return findChildByName(findChildIds, name)
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
            upsertChildren(children = children)
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
            upsertChildren(children = children.asSequence())
        }
    }

    private fun readChildrenRefs(): Sequence<BlockAddress> {
        val dataRef = readDataRef() ?: return sequenceOf()
        return readChildrenRefs(dataRef)
    }

    private fun readChildrenRefs(ref: BlockAddress): Sequence<BlockAddress> {
        val lock = MemoryFileLock.unlocked(Mode.read())
        val channel = context.ledger.existingChannel(ref = ref, lock = lock)
        channel.use {
            return context.schemas.refListHandler
                .read(it)
                .asSequence()
        }
    }

    private fun findChildByName(refs: Sequence<BlockAddress>, name: String): NodeMapper? {
        for (id in refs) {
            val readNode = context.mappers.readNode(id)
            if (readNode.readName() == name) {
                return readNode
            }
        }
        return null
    }

    private fun upsertChildren(children: Sequence<BlockAddress>) {
        val prevDataRef = readDataRef()
        if (prevDataRef != null && prevDataRef.isValid()) {
            context.ledger.releaseAll(ref = prevDataRef)
        }

        val childrenCount = children.count()
        val newDataRef: BlockAddress = if (childrenCount > 0) {
            val refSequenceHandler = context.schemas.refListHandler
            val exactBodySize = refSequenceHandler.exactBodySize(childrenCount)
            val newDataByteChannel = context.ledger.allocateChannel(exactBodySize = exactBodySize)
            newDataByteChannel.use {
                refSequenceHandler.write(buffer = it, value = children.toList())
            }
            newDataByteChannel.address()
        } else {
            BlockAddress.InvalidAddress
        }
        writeDataRef(newDataRef)
    }

    fun readAttrsRef(): BlockAddress {
        throwIfNotReadable()
        val property = schema.attrsRef
        property.setPosition(buffer)
        return property.read(buffer)
    }

    fun writeAttrsRef(ref: BlockAddress) {
        throwIfNotWritable()
        val property = schema.attrsRef
        property.write(buffer, ref)
    }

    fun readAttrs(): AttrsMapper {
        return context.mappers.readAttrs(readAttrsRef())
    }

    fun readNameRef(): BlockAddress {
        throwIfNotReadable()
        val property = schema.nameRef
        property.setPosition(buffer)
        return property.read(buffer)
    }

    fun writeNameRef(ref: BlockAddress) {
        throwIfNotWritable()
        val property = schema.nameRef
        property.setPosition(buffer)
        property.write(buffer, ref)
    }

    fun readName(): String {
        val address = readNameRef()
        val mode1 = Mode.read()
        val lock = MemoryFileLock.unlocked(mode1)
        val channel = context.ledger.existingChannel(ref = address, lock = lock)
        channel.use {
            return context.schemas.stringHandler.read(it)
        }
    }

    override fun flip(mode: OpenMode): BlockAddress {
        throwIfNotWritable()
        checkPosition(schema.addressRange)

        if (tryFlip()) {
            this.ref = buffer.flip()
            return checkNotNull(this.ref)
        } else {
            throw MemoryIllegalStateException()
        }
    }

    private fun checkPosition(range: ClosedRange<BlockAddress>) {
        Check.isTrue { BlockAddress(buffer.position()) in range }
    }

}