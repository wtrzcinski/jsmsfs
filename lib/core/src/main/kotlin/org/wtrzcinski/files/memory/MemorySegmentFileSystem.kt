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

package org.wtrzcinski.files.memory

import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.exception.MemoryIllegalArgumentException
import org.wtrzcinski.files.memory.lock.MemoryFileLock.Companion.use
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.intByteSize
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.ModeState
import org.wtrzcinski.files.memory.node.*
import org.wtrzcinski.files.memory.provider.MemoryFileOpenOptions
import org.wtrzcinski.files.memory.provider.MemoryFileOpenOptions.Companion.WRITE_TRUNCATE
import org.wtrzcinski.files.memory.util.Check
import org.wtrzcinski.files.memory.util.Require
import java.io.File
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.NoSuchFileException
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.attribute.PosixFilePermissions.toString
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.reflect.KClass
import kotlin.reflect.cast
import kotlin.reflect.full.isSuperclassOf

@OptIn(ExperimentalAtomicApi::class)
@Suppress("unused")
class MemorySegmentFileSystem(
    val context: MemorySegmentContext,
    name: String = "",
) : AutoCloseable, ModeState(
    mode = Mode.of(readOnly = context.ledger.memory.isReadOnly())
) {

    fun isAlive(): Boolean {
        return context.ledger.memory.scope().isAlive()
    }

    override fun close() {
        if (tryClose()) {
            context.close()
        }
    }

    val rootRef: BlockOffset = run {
        val ref = getOrCreateFile(
            parent = null,
            childType = NodeType.Directory,
            childName = File.separator,
            mode = MemoryFileOpenOptions.REQUIRE_NEW,
        )
        ref
    }

    fun root(): DirectoryNode {
        return read(type = DirectoryNode::class, nodeRef = rootRef)
    }

    fun getOrCreateData(
        parent: DirectoryNode,
        childName: String,
        mode: MemoryFileOpenOptions
    ): MemoryReadWriteBuffer {
        checkIsWritable()

        val child = getOrCreateFile(
            parent = parent,
            childType = NodeType.Regular,
            childName = childName,
            mode = mode,
        )

        var dataSegmentRef: BlockOffset? = readDataOffset(child)

        val childLock = context.locks.newLock(offset = child, mode = mode)
        childLock.acquire()
        try {
            if (dataSegmentRef == null) {
                dataSegmentRef = readDataOffset(child)
                if (dataSegmentRef == null) {
                    Check.isTrue { mode.readWrite }

                    val dataSegment = context.ledger.allocateChannel(lock = childLock)
                    updateDataOffset(nodeRef = child, newDataRef = dataSegment.address())
                    return dataSegment
                } else {
                    return context.ledger.existingChannel(
                        mode = mode.mode,
                        offset = dataSegmentRef,
                        lock = childLock
                    )
                }
            } else {
                return context.ledger.existingChannel(
                    mode = mode.mode,
                    offset = dataSegmentRef,
                    lock = childLock
                )
            }
        } catch (e: Exception) {
            childLock.release()

            throw e
        }
    }

    fun getOrCreateFile(
        parent: DirectoryNode?,
        childType: NodeType,
        childName: String,
        mode: MemoryFileOpenOptions,
        targetNode: ValidNode? = null,
    ): BlockOffset {
        checkIsWritable()

        Require.notEmpty(childName)

        val existingChild = findChildByName(parent, childName)
        if (existingChild != null) {
            requireNew(mode, childName)
            return existingChild.offset
        }

        val parentLock = context.locks.newLock(offset = parent?.offset, mode = WRITE_TRUNCATE)
        return parentLock.use {
            val existingChild = findChildByName(parent, childName)
            if (existingChild != null) {
                requireNew(mode, childName)
                return@use existingChild.offset
            }
            if (!mode.create) {
                throw NoSuchFileException(childName)
            }

            return@use createFile(childName, childType, targetNode, parent)
        }
    }

    private fun createFile(name: String, type: NodeType, target: ValidNode? = null, parent: DirectoryNode? = null): BlockOffset {
        val nameMapper = context.mappers.createString(name)
        val nameOffset = nameMapper.flip()
        nameMapper.close()

        val attrsMapper = context.mappers.createAttrs()
        val attrsOffset = attrsMapper.flip()
        attrsMapper.close()

        val nodeMapper = context.mappers.createFile()
        nodeMapper.writeType(type)
        if (target != null) {
            val dataOffset = readDataOffset(target.offset)
            if (dataOffset != null) {
                nodeMapper.writeDataOffset(dataOffset)
            } else {
                nodeMapper.writeDataOffset(BlockOffset.InvalidOffset)
            }
        } else {
            nodeMapper.writeDataOffset(BlockOffset.InvalidOffset)
        }
        nodeMapper.writeAttrsOffset(attrsOffset)
        nodeMapper.writeNameOffset(nameOffset)
        val nodeOffset = nodeMapper.flip()
        nodeMapper.close()

        if (parent != null) {
            addChild(parent = parent, childRef = nodeOffset)
        }

        return nodeOffset
    }

    fun <T : Any> read(type: KClass<T>, nodeRef: BlockOffset): T {
        val node = context.ledger.existingChannel(mode = Mode.readOnly(), offset = nodeRef)
        node.use {
            if (ValidNode::class.isSuperclassOf(type)) {
                val fileTypeOrdinal = node.readInt()
                val dataRef = node.readOffset()
                val attrRef = node.readOffset()
                val nameRef = node.readOffset()
                requireNotNull(nameRef)
                requireNotNull(attrRef)

                val nameChannel = context.ledger.existingChannel(mode = Mode.readOnly(), offset = nameRef)
                val name = nameChannel.use {
                    it.readString()
                }

                val fileType = NodeType.entries[fileTypeOrdinal]
                when (fileType) {
                    NodeType.Directory -> {
                        return type.cast(
                            DirectoryNode(
                                nodeRef = nodeRef,
                                dataRef = dataRef ?: BlockOffset.InvalidOffset,
                                attrRef = attrRef,
                                nameRef = nameRef,
                                name = name,
                            )
                        )
                    }

                    NodeType.Regular -> {
                        return type.cast(
                            RegularFileNode(
                                nodeRef = nodeRef,
                                dataRef = dataRef ?: BlockOffset.InvalidOffset,
                                attrRef = attrRef,
                                nameRef = nameRef,
                                name = name,
                            )
                        )
                    }

                    NodeType.SymbolicLink -> {
                        return type.cast(
                            SymbolicLinkNode(
                                nodeRef = nodeRef,
                                dataRef = dataRef ?: BlockOffset.InvalidOffset,
                                attrRef = attrRef,
                                nameRef = nameRef,
                                name = name,
                            )
                        )
                    }

                    else -> throw MemoryIllegalArgumentException()
                }
            }
            throw MemoryIllegalArgumentException()
        }
    }

    fun delete(parent: NamedNode, child: ValidNode) {
        Check.isTrue { child.offset != this.rootRef }

        require(parent is DirectoryNode)
        val parentLock = context.locks.newLock(offset = parent.offset, mode = WRITE_TRUNCATE)
        parentLock.use {
            removeChildByName(parent, child.name)
        }

        val childLock = context.locks.newLock(offset = child.offset, mode = WRITE_TRUNCATE)
        childLock.use {
            if (child is DirectoryNode) {
                val readChildIds = readChildrenRefs(child)
                val hasChildren = readChildIds.iterator().hasNext()
                if (hasChildren) {
                    throw DirectoryNotEmptyException(child.name)
                }
            }
            if (child is RegularFileNode || child is DirectoryNode) {
                val dataRef = readDataOffset(child.offset)
                if (dataRef != null) {
                    context.ledger.releaseAll(dataRef)
                }
            }

            val attrRef = readAttrsOffset(child.offset)
            if (attrRef != null) {
                context.ledger.releaseAll(attrRef)
            }

            val nameOffset = readNameOffset(child.offset)
            if (nameOffset != null) {
                context.ledger.releaseAll(nameOffset)
            }

            context.ledger.releaseAll(child.offset)
        }
    }

    //    data
    private fun readDataOffset(offset: BlockOffset): BlockOffset? {
        val read = read(type = ValidNode::class, nodeRef = offset)
        val dataRef = read.dataRef
        return if (dataRef.isValid()) {
            dataRef
        } else {
            null
        }
    }

    //    attrs
    fun findAttrs(nodeOffset: BlockOffset): AttributesBlock {
        val attrRef = readAttrsOffset(start = nodeOffset)
        requireNotNull(attrRef)

        return readAttrs(attrsRef = attrRef)
    }

    fun updateFileTime(offset: BlockOffset, attrs: AttributesBlock) {
        context.locks.newLock(offset = offset, mode = WRITE_TRUNCATE).use {
            val attrsRef = readAttrsOffset(offset)
            requireNotNull(attrsRef)

            val attrsByteChannel = context.ledger.existingChannel(mode = Mode.updateRead(), offset = attrsRef)
            attrsByteChannel.use {
                it.writeInstant(attrs.lastAccessTime)
                it.writeInstant(attrs.lastModifiedTime)
                it.skipRemaining()
            }
        }
    }

    fun updatePermissions(offset: BlockOffset, attrs: AttributesBlock) {
        context.locks.newLock(offset = offset, mode = WRITE_TRUNCATE).use {
            val attrsRef = readAttrsOffset(offset)
            requireNotNull(attrsRef)

            val attrsByteChannel = context.ledger.existingChannel(mode = Mode.updateRead(), offset = attrsRef)
            attrsByteChannel.use {
                it.readInstant()
                it.readInstant()
                it.readInstant()
                it.writeString(toString(attrs.permissions))
                it.skipRemaining()
            }
        }
    }

    private fun readAttrs(attrsRef: BlockOffset): AttributesBlock {
        val attrsByteChannel = context.ledger.existingChannel(mode = Mode.readOnly(), offset = attrsRef)
        attrsByteChannel.use {
            val accessed = it.readInstant()
            val modified = it.readInstant()
            val created = it.readInstant()
            val permissions = it.readString()
            val owner = it.readString()
            val group = it.readString()
            return AttributesBlock(
                lastAccessTime = accessed,
                lastModifiedTime = modified,
                creationTime = created,
                permissions = PosixFilePermissions.fromString(permissions),
                owner = owner,
                group = group,
            )
        }
    }

    private fun readAttrsOffset(start: BlockOffset): BlockOffset? {
        val read = read(type = ValidNode::class, nodeRef = start)
        val attrRef = read.attrsRef
        return if (attrRef.isValid()) {
            attrRef
        } else {
            null
        }
    }

    private fun readNameOffset(start: BlockOffset): BlockOffset? {
        val read = read(type = ValidNode::class, nodeRef = start)
        val nameRef = read.nameRef
        return if (nameRef.isValid()) {
            nameRef
        } else {
            null
        }
    }

    //    children
    fun findChildren(node: DirectoryNode): Sequence<ValidNode> {
        val readChildrenRefs = readChildrenRefs(node)
        return readChildrenRefs.map { read(type = ValidNode::class, nodeRef = it) }
    }

    fun findChildByName(parent: DirectoryNode?, name: String): ValidNode? {
        if (parent == null) {
            return null
        }
        val findChildIds = readChildrenRefs(parent)
        return findChildByName(findChildIds, name)
    }

    private fun findChildByName(ids: Sequence<BlockOffset>, name: String): ValidNode? {
        for (id in ids) {
            val node = read(type = ValidNode::class, nodeRef = id)
            if (node.name == name) {
                return node
            }
        }
        return null
    }

    private fun addChild(parent: DirectoryNode, childRef: BlockOffset) {
        context.locks.newLock(offset = parent.offset, mode = WRITE_TRUNCATE).use {
            val children: Sequence<BlockOffset> = readChildrenRefs(parent) + childRef

            upsertChildren(nodeRef = parent.offset, prevDataRef = parent.dataRef, children = children)
        }
    }

    private fun removeChildByName(parent: DirectoryNode, name: String) {
        val childIds = readChildrenRefs(parent)
        val findChildByName = findChildByName(childIds, name)
        require(findChildByName != null)

        val children = mutableListOf<BlockOffset>()
        children.addAll(childIds)
        children.remove(findChildByName.offset)
        upsertChildren(nodeRef = parent.offset, prevDataRef = parent.dataRef, children = children.asSequence())
    }

    private fun readChildrenRefs(parent: DirectoryNode): Sequence<BlockOffset> {
        val dataRef = readDataOffset(parent.offset)
        if (dataRef != null) {
            return readChildrenRefs(dataRef)
        }
        return sequenceOf()
    }

    private fun upsertChildren(nodeRef: BlockOffset, prevDataRef: BlockOffset, children: Sequence<BlockOffset>) {
        if (prevDataRef.isValid()) {
            context.ledger.releaseAll(offset = prevDataRef)
        }

        val childrenCount = children.count()
        val newDataRef: BlockOffset = if (childrenCount > 0) {
            val maxExpectedBodySize = intByteSize + (context.ledger.offsetBytes * childrenCount)
            val newDataByteChannel = context.ledger.allocateChannel(bodyAlignment = maxExpectedBodySize)
            newDataByteChannel.use {
                it.writeOffsets(children)
            }
            newDataByteChannel.address()
        } else {
            BlockOffset.InvalidOffset
        }
        updateDataOffset(nodeRef, newDataRef)
    }

    private fun updateDataOffset(nodeRef: BlockOffset, newDataRef: BlockOffset) {
        val nodeSegmentChannel = context.ledger.existingChannel(mode = Mode.updateRead(), offset = nodeRef)
        nodeSegmentChannel.use {
            it.skipInt()
            it.writeOffset(newDataRef)
            it.skipRemaining()
        }
    }

    private fun readChildrenRefs(dataRef: BlockOffset): Sequence<BlockOffset> {
        val childrenByteChannel = context.ledger.existingChannel(mode = Mode.readOnly(), offset = dataRef)
        childrenByteChannel.use {
            return it.readRefs()
        }
    }

    private fun requireNew(mode: MemoryFileOpenOptions, childName: String) {
        if (mode.requireNew) {
            throw FileAlreadyExistsException(childName)
        }
    }
}