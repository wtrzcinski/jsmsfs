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

import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.allocator.MemoryScopeType
import org.wtrzcinski.files.memory.allocator.MemorySegmentFactory
import org.wtrzcinski.files.memory.bitmap.BitmapRegistry
import org.wtrzcinski.files.memory.bitmap.BitmapRegistryGroup
import org.wtrzcinski.files.memory.buffer.MemoryOpenOptions
import org.wtrzcinski.files.memory.buffer.MemoryOpenOptions.Companion.READ
import org.wtrzcinski.files.memory.buffer.MemoryOpenOptions.Companion.WRITE
import org.wtrzcinski.files.memory.buffer.channel.FragmentedReadWriteBuffer
import org.wtrzcinski.files.memory.exception.MemoryIllegalArgumentException
import org.wtrzcinski.files.memory.exception.MemoryIllegalFileNameException
import org.wtrzcinski.files.memory.lock.MemoryFileLock.Companion.use
import org.wtrzcinski.files.memory.lock.MemoryLockRegistry
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.intByteSize
import org.wtrzcinski.files.memory.node.*
import org.wtrzcinski.files.memory.node.attribute.AttributesBlock
import org.wtrzcinski.files.memory.util.AbstractCloseable
import java.io.File
import java.lang.foreign.MemorySegment
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.NoSuchFileException
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.attribute.PosixFilePermissions.toString
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.reflect.KClass
import kotlin.reflect.cast
import kotlin.reflect.full.isSuperclassOf

@OptIn(ExperimentalAtomicApi::class)
internal class MemorySegmentFileSystem(
    scope: MemoryScopeType,
    val byteSize: ByteSize,
    blockSize: ByteSize,
    val name: String = "",
    env: Map<String, Any?> = mapOf(),
) : AutoCloseable, AbstractCloseable() {

    val lockRegistry = MemoryLockRegistry()

    val memoryFactory: MemorySegmentFactory = scope.createFactory(env)

    val memorySegment: MemorySegment = memoryFactory.allocate(byteSize.size)

    val bitmapRegistry: BitmapRegistryGroup = BitmapRegistry(memoryOffset = 0L, memorySize = byteSize, readOnly = false, lockRegistry = lockRegistry)

    val memory = MemoryLedger(memory = memorySegment, bitmap = bitmapRegistry, maxBlockByteSize = blockSize)

    val mapperRegistry = MemoryMapperRegistry(memory)

    override fun close() {
        if (tryClose()) {
            memoryFactory.close()
            lockRegistry.close()
        }
    }

    val rootRef: BlockStart = run {
        getOrCreateFile(
            parent = null,
            childType = NodeType.Directory,
            childName = File.separator,
            mode = MemoryOpenOptions.CREATE_NEW,
            targetNode = null,
        )
    }

    fun root(): DirectoryNode {
        return read(type = DirectoryNode::class, nodeRef = rootRef)
    }

    fun getOrCreateDataChannel(
        parent: DirectoryNode,
        childName: String,
        mode: MemoryOpenOptions
    ): FragmentedReadWriteBuffer {
        checkIsOpen()

        val child = getOrCreateFile(
            parent = parent,
            childType = NodeType.Regular,
            childName = childName,
            mode = mode,
            targetNode = null,
        )

        var dataSegmentRef: BlockStart? = readDataOffset(child)
        val childLock = lockRegistry.newLock(offset = child, mode = mode)
        childLock.acquire()
        if (dataSegmentRef == null) {
            dataSegmentRef = readDataOffset(child)
            if (dataSegmentRef == null) {
                require(mode.write)

                val dataSegment = memory.newByteChannel(mode = mode, lock = childLock)
                updateDataOffset(nodeRef = child, newDataRef = dataSegment.first())
                return dataSegment
            } else {
                val dataByteChannel = memory.existingByteChannel(mode = mode, offset = dataSegmentRef, lock = childLock)
                if (mode.append) {
                    dataByteChannel.append()
                } else {
                    dataByteChannel.truncate()
                }
                return dataByteChannel
            }
        } else {
            val dataByteChannel = memory.existingByteChannel(mode = mode, offset = dataSegmentRef, lock = childLock)
            if (mode.append) {
                dataByteChannel.append()
            } else {
                dataByteChannel.truncate()
            }
            return dataByteChannel
        }
    }

    fun getOrCreateFile(
        parent: DirectoryNode?,
        childType: NodeType,
        childName: String,
        mode: MemoryOpenOptions,
        targetNode: ValidNode?,
    ): BlockStart {
        checkIsOpen()

        if (childName.isEmpty()) {
            throw MemoryIllegalFileNameException()
        }

        val existingChild = findChildByName(parent, childName)
        if (existingChild != null) {
            if (mode.createNew) {
                throw FileAlreadyExistsException(childName)
            }
            return existingChild.offset
        }

        val parentLock = lockRegistry.newLock(offset = parent?.offset, mode = WRITE)
        return parentLock.use {
            val existingChild = findChildByName(parent, childName)
            if (existingChild != null) {
                if (mode.createNew) {
                    throw FileAlreadyExistsException(childName)
                }
                return@use existingChild.offset
            }
            if (!mode.create) {
                throw NoSuchFileException(childName)
            }

//            attrs
            val attrsMapper = mapperRegistry.startAttrs()
            val attrs = AttributesBlock(now = Instant.now())
            attrsMapper.writeLastAccessTime(attrs.lastAccessTime)
            attrsMapper.writeLastModifiedTime(attrs.lastModifiedTime)
            attrsMapper.writeCreationTime(attrs.creationTime)
            attrsMapper.writePermissions(attrs.permissions)
            attrsMapper.writeOwner(attrs.owner)
            attrsMapper.writeGroup(attrs.group)
            val attrsOffset = attrsMapper.flip()

//            name
            val nameMapper = mapperRegistry.startName()
            nameMapper.writeName(childName)
            val nameOffset = nameMapper.flip()

//            node
            val nodeMapper = mapperRegistry.startNode()
            nodeMapper.writeType(childType)
            if (targetNode != null) {
                val dataOffset = readDataOffset(targetNode.offset)
                if (dataOffset != null) {
                    nodeMapper.writeDataOffset(dataOffset)
                } else {
                    nodeMapper.writeDataOffset(BlockStart.InvalidAddress)
                }
            } else {
                nodeMapper.writeDataOffset(BlockStart.InvalidAddress)
            }
            nodeMapper.writeAttrOffset(attrsOffset)
            nodeMapper.writeNameOffset(nameOffset)
            val nodeOffset = nodeMapper.flip()

//            parent
            if (parent != null) {
                addChild(parent = parent, childRef = nodeOffset)
            }

            return@use nodeOffset
        }
    }

    fun <T : Any> read(type: KClass<T>, nodeRef: BlockStart): T {
        val node = memory.existingByteChannel(mode = READ, offset = nodeRef)
        node.use {
            if (ValidNode::class.isSuperclassOf(type)) {
                val fileTypeOrdinal = node.readInt()
                val dataRef = node.readOffset()
                val attrRef = node.readOffset()
                val nameRef = node.readOffset()
                requireNotNull(nameRef)
                requireNotNull(attrRef)

                val nameChannel = memory.existingByteChannel(mode = READ, offset = nameRef)
                val name = nameChannel.use {
                    it.readString()
                }

                val fileType = NodeType.entries[fileTypeOrdinal]
                when (fileType) {
                    NodeType.Directory -> {
                        return type.cast(
                            DirectoryNode(
                                nodeRef = nodeRef,
                                dataRef = dataRef ?: BlockStart.InvalidAddress,
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
                                dataRef = dataRef ?: BlockStart.InvalidAddress,
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
                                dataRef = dataRef ?: BlockStart.InvalidAddress,
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

    fun delete(parent: NamedNode?, child: ValidNode) {
        if (child.offset == this.rootRef) {
            return
        }

        require(parent is DirectoryNode)
        val parentLock = lockRegistry.newLock(offset = parent.offset, mode = WRITE)
        parentLock.use {
            removeChildByName(parent, child.name)
        }

        val childLock = lockRegistry.newLock(offset = child.offset, mode = WRITE)
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
                    memory.release(dataRef)
                }
            }

            val attrRef = readAttrsOffset(child.offset)
            if (attrRef != null) {
                memory.release(attrRef)
            }

            val nameOffset = readNameOffset(child.offset)
            if (nameOffset != null) {
                memory.release(nameOffset)
            }

            memory.release(child.offset)
        }
    }

    //    data
    private fun readDataOffset(offset: BlockStart): BlockStart? {
        val read = read(type = ValidNode::class, nodeRef = offset)
        val dataRef = read.dataRef
        return if (dataRef.isValid()) {
            dataRef
        } else {
            null
        }
    }

    //    attrs
    fun findAttrs(node: ValidNode): AttributesBlock {
        val attrRef = readAttrsOffset(start = node.offset)
        requireNotNull(attrRef)

        return readAttrs(attrsRef = attrRef)
    }

    fun updateFileTime(offset: BlockStart, attrs: AttributesBlock) {
        lockRegistry.newLock(offset = offset, mode = WRITE).use {
            val attrsRef = readAttrsOffset(offset)
            requireNotNull(attrsRef)

            val attrsByteChannel = memory.existingByteChannel(mode = WRITE, offset = attrsRef)
            attrsByteChannel.use {
                it.writeInstant(attrs.lastAccessTime)
                it.writeInstant(attrs.lastModifiedTime)
                it.skipRemaining()
            }
        }
    }

    fun updatePermissions(offset: BlockStart, attrs: AttributesBlock) {
        lockRegistry.newLock(offset = offset, mode = WRITE).use {
            val attrsRef = readAttrsOffset(offset)
            requireNotNull(attrsRef)

            val attrsByteChannel = memory.existingByteChannel(mode = WRITE, offset = attrsRef)
            attrsByteChannel.use {
                it.readInstant()
                it.readInstant()
                it.readInstant()
                it.writeString(toString(attrs.permissions))
                it.skipRemaining()
            }
        }
    }

    private fun readAttrs(attrsRef: BlockStart): AttributesBlock {
        val attrsByteChannel = memory.existingByteChannel(mode = READ, offset = attrsRef)
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

    private fun readAttrsOffset(start: BlockStart): BlockStart? {
        val read = read(type = ValidNode::class, nodeRef = start)
        val attrRef = read.attrsRef
        return if (attrRef.isValid()) {
            attrRef
        } else {
            null
        }
    }

    private fun readNameOffset(start: BlockStart): BlockStart? {
        val read = read(type = ValidNode::class, nodeRef = start)
        val nameRef = read.nameRef
        return if (nameRef.isValid()) {
            nameRef
        } else {
            null
        }
    }

    //    children
    fun findChildren(parent: DirectoryNode): Sequence<ValidNode> {
        return readChildrenRefs(parent).map { read(type = ValidNode::class, nodeRef = it) }
    }

    fun findChildByName(parent: DirectoryNode?, name: String): ValidNode? {
        if (parent == null) {
            return null
        }
        val findChildIds = readChildrenRefs(parent)
        return findChildByName(findChildIds, name)
    }

    private fun findChildByName(ids: Sequence<BlockStart>, name: String): ValidNode? {
        for (id in ids) {
            val node = read(type = ValidNode::class, nodeRef = id)
            if (node.name == name) {
                return node
            }
        }
        return null
    }

    private fun addChild(parent: DirectoryNode, childRef: BlockStart) {
        lockRegistry.newLock(offset = parent.offset, mode = WRITE).use {
            val children: Sequence<BlockStart> = readChildrenRefs(parent) + childRef

            upsertChildren(nodeRef = parent.offset, prevDataRef = parent.dataRef, children = children)
        }
    }

    private fun removeChildByName(parent: DirectoryNode, name: String) {
        val childIds = readChildrenRefs(parent)
        val findChildByName = findChildByName(childIds, name)
        require(findChildByName != null)

        val children = mutableListOf<BlockStart>()
        children.addAll(childIds)
        children.remove(findChildByName.offset)
        upsertChildren(nodeRef = parent.offset, prevDataRef = parent.dataRef, children = children.asSequence())
    }

    private fun readChildrenRefs(parent: DirectoryNode): Sequence<BlockStart> {
        val dataRef = readDataOffset(parent.offset) ?: return sequenceOf()
        return readChildrenRefs(dataRef)
    }

    private fun upsertChildren(nodeRef: BlockStart, prevDataRef: BlockStart, children: Sequence<BlockStart>) {
        if (prevDataRef.isValid()) {
            memory.release(offset = prevDataRef)
        }

        val childrenCount = children.count()
        val newDataRef: BlockStart = if (childrenCount > 0) {
            val maxExpectedBodySize = intByteSize + (memory.offsetBytes * childrenCount)
            val newDataByteChannel = memory.newByteChannel(bodySize = maxExpectedBodySize)
            newDataByteChannel.use {
                it.writeOffsets(children)
            }
            newDataByteChannel.first()
        } else {
            BlockStart.InvalidAddress
        }
        updateDataOffset(nodeRef, newDataRef)
    }

    private fun updateDataOffset(nodeRef: BlockStart, newDataRef: BlockStart) {
        val nodeSegmentChannel = memory.existingByteChannel(mode = WRITE, offset = nodeRef)
        nodeSegmentChannel.use {
            it.skipInt()
            it.writeOffset(newDataRef)
            it.skipRemaining()
        }
    }

    private fun readChildrenRefs(dataRef: BlockStart): Sequence<BlockStart> {
        val childrenByteChannel = memory.existingByteChannel(mode = READ, offset = dataRef)
        childrenByteChannel.use {
            return it.readRefs()
        }
    }
}