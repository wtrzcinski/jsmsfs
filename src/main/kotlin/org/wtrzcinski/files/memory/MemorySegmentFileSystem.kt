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

import org.wtrzcinski.files.memory.attribute.AttributesBlock
import org.wtrzcinski.files.memory.bitmap.BitmapRegistry
import org.wtrzcinski.files.memory.bitmap.BitmapRegistryGroup
import org.wtrzcinski.files.memory.block.MemoryBlock
import org.wtrzcinski.files.memory.block.MemoryBlockRegistry
import org.wtrzcinski.files.memory.channel.MemoryOpenOptions
import org.wtrzcinski.files.memory.channel.MemorySeekableByteChannel
import org.wtrzcinski.files.memory.directory.DirectoryNode
import org.wtrzcinski.files.memory.exception.MemoryIllegalArgumentException
import org.wtrzcinski.files.memory.exception.MemoryIllegalFileNameException
import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.lock.MemoryFileLock.Companion.use
import org.wtrzcinski.files.memory.node.Node
import org.wtrzcinski.files.memory.node.NodeType
import org.wtrzcinski.files.memory.node.RegularFileNode
import org.wtrzcinski.files.memory.node.ValidNode
import org.wtrzcinski.files.memory.ref.BlockStart
import java.io.File
import java.lang.foreign.MemorySegment
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.NoSuchFileException
import java.nio.file.attribute.PosixFilePermissions
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.reflect.KClass
import kotlin.reflect.cast
import kotlin.reflect.full.isSuperclassOf
import kotlin.use

@OptIn(ExperimentalAtomicApi::class)
internal class MemorySegmentFileSystem(
    scope: MemoryScopeType,
    val byteSize: Long,
    blockSize: Long,
    val name: String = "",
    env: Map<String, Any?> = mapOf(),
) : AutoCloseable {

    val memoryFactory: MemorySegmentFactory = scope.createFactory(env)

    val memory: MemorySegment = memoryFactory.allocate(byteSize)

    val bitmapStore: BitmapRegistryGroup = BitmapRegistry(memoryOffset = 0L, memorySize = byteSize, readOnly = false)

    val blockStore: MemoryBlockRegistry =
        MemoryBlockRegistry(memory = memory, bitmap = bitmapStore, maxMemoryBlockByteSize = blockSize)

    private val closed = AtomicBoolean(false)

    override fun close() {
        if (closed.compareAndSet(expectedValue = false, newValue = true)) {
            memoryFactory.close()
        }
    }

    private fun checkAccessible() {
        if (closed.load()) {
            throw MemoryIllegalStateException()
        }
    }

    val rootRef: BlockStart = run {
        val directory = getOrCreateFile(
            parent = null,
            childType = NodeType.Directory,
            childName = File.separator,
            mode = MemoryOpenOptions.CREATE_NEW
        )
        directory.nodeRef

    }

    fun root(): DirectoryNode {
        return read(type = DirectoryNode::class, nodeRef = rootRef)
    }

    fun getOrCreateDataChannel(
        parent: DirectoryNode,
        childName: String,
        mode: MemoryOpenOptions
    ): MemorySeekableByteChannel {
        checkAccessible()

        val child = getOrCreateFile(
            parent = parent,
            childType = NodeType.RegularFile,
            childName = childName,
            mode = mode,
        )

        var dataSegment: MemoryBlock? = null

        val childLock: MemoryFileLock = blockStore.lock(offset = child.nodeRef)
        var dataSegmentRef: BlockStart? = readDataRef(child.nodeRef)
        if (dataSegmentRef == null) {
            childLock.use(mode = MemoryOpenOptions.WRITE) {
                dataSegmentRef = readDataRef(child.nodeRef)
                if (dataSegmentRef == null) {
                    dataSegment = blockStore.reserveSegment()
                    updateDataRef(nodeRef = child.nodeRef, newDataRef = dataSegment)
                    dataSegmentRef = BlockStart.of(dataSegment.start)
                }
            }
        }
        require(dataSegmentRef != null)

        childLock.acquire(mode)
        try {
            if (dataSegment != null) {
//                the data segment was just created
                return dataSegment.newByteChannel(mode, childLock)
            } else {
//                the data segment was already present
                dataSegment = blockStore.findSegment(dataSegmentRef)
                val dataByteChannel = dataSegment.newByteChannel(mode, childLock)
                if (mode.append) {
//                    existing bytes need to be skipped
                    dataByteChannel.skipRemaining()
                }
                return dataByteChannel
            }
        } catch (e: Exception) {
            childLock.release(mode)
            throw e
        }
    }

    fun getOrCreateFile(
        parent: DirectoryNode?,
        childType: NodeType,
        childName: String,
        mode: MemoryOpenOptions
    ): ValidNode {
        checkAccessible()

        if (childName.isEmpty()) {
            throw MemoryIllegalFileNameException()
        }

        val existingChild = findChildByName(parent, childName)
        if (existingChild != null) {
            if (mode.createNew) {
                throw FileAlreadyExistsException(childName)
            }
            return existingChild
        }

        val parentLock = blockStore.lock(offset = parent?.nodeRef ?: BlockStart.Invalid)
        return parentLock.use(MemoryOpenOptions.WRITE) {
            val existingChild = findChildByName(parent, childName)
            if (existingChild != null) {
                if (mode.createNew) {
                    throw FileAlreadyExistsException(childName)
                }
                return@use existingChild
            }
            if (!mode.create) {
                throw NoSuchFileException(childName)
            }
            val now = java.time.Instant.now()
            val attrsRef: BlockStart = createAttrs(
                AttributesBlock(
                    lastAccessTime = now,
                    lastModifiedTime = now,
                    creationTime = now,
                )
            )
            val childRef = createFile(
                parent = parent,
                type = childType,
                childName = childName,
                attrsRef = attrsRef,
            )
            return@use ValidNode(
                nodeRef = childRef,
                fileType = childType,
                name = childName,
                attrsRef = attrsRef,
            )
        }
    }

    private fun createFile(parent: DirectoryNode?, type: NodeType, childName: String, attrsRef: BlockStart): BlockStart {
        val newChildSegment = blockStore.reserveSegment(tag = childName)
        val newChildByteChannel = newChildSegment.newByteChannel(mode = MemoryOpenOptions.WRITE, lock = null)
        newChildByteChannel.use {
            it.writeInt(type.ordinal)
            it.writeRef(BlockStart.Invalid)
            it.writeRef(attrsRef)
            it.writeString(childName)
        }
        val childRef = newChildByteChannel.offset()
        if (parent != null) {
            addChild(parent, childRef)
        }
        return childRef
    }

    fun <T : Any> read(type: KClass<T>, nodeRef: BlockStart): T {
        val segment = blockStore.findSegment(offset = nodeRef)
        val node = segment.newByteChannel(mode = MemoryOpenOptions.READ, lock = null)
        node.use {
            if (type.isSuperclassOf(DirectoryNode::class) || type.isSuperclassOf(RegularFileNode::class)) {
                val fileTypeOrdinal = node.readInt()
                val dataRef = node.readRef()
                val attrRef = node.readRef()
                val name = node.readString()
                val fileType = NodeType.entries[fileTypeOrdinal]
                if (fileType == NodeType.Directory) {
                    return type.cast(
                        DirectoryNode(
                            nodeRef = nodeRef,
                            dataRef = dataRef,
                            attrRef = attrRef,
                            name = name,
                        )
                    )
                } else if (fileType == NodeType.RegularFile) {
                    return type.cast(
                        RegularFileNode(
                            nodeRef = nodeRef,
                            dataRef = dataRef,
                            attrRef = attrRef,
                            name = name,
                        )
                    )
                }
            }
            throw MemoryIllegalArgumentException()
        }
    }

    fun delete(parent: Node?, child: ValidNode) {
        if (child.nodeRef == this.rootRef) {
            return
        }

        if (child is DirectoryNode) {
            val readChildIds = readChildIds(child)
            val hasChildren = readChildIds.iterator().hasNext()
            if (hasChildren) {
                throw DirectoryNotEmptyException(child.name)
            }
        }

        require(parent is DirectoryNode)
        val parentLock = blockStore.lock(parent.nodeRef)
        parentLock.use(MemoryOpenOptions.WRITE) {
            removeChildByName(parent, child.name)
        }

        val childLock = blockStore.lock(child.nodeRef)
        childLock.use(MemoryOpenOptions.WRITE) {
            val dataRef = readDataRef(child.nodeRef)
            if (dataRef != null) {
                blockStore.releaseAll(dataRef)
            }

            val attrRef = readAttrsRef(child.nodeRef)
            if (attrRef != null) {
                blockStore.releaseAll(attrRef)
            }

            blockStore.releaseAll(child.nodeRef)
        }
    }

    //    data
    private fun updateDataRef(nodeRef: BlockStart, newDataRef: BlockStart) {
        val nodeSegment = blockStore.findSegment(nodeRef)
        val nodeSegmentChannel = nodeSegment.newByteChannel(mode = MemoryOpenOptions.WRITE, lock = null)
        nodeSegmentChannel.use {
            it.readInt()
            it.writeRef(newDataRef)
            it.skipRemaining()
        }
    }

    private fun readDataRef(nodeRef: BlockStart): BlockStart? {
        val read = read(type = ValidNode::class, nodeRef = nodeRef)
        val dataRef = read.dataRef
        return if (dataRef.isValid()) {
            dataRef
        } else {
            null
        }
    }

    //    attrs
    fun findAttrs(node: ValidNode): AttributesBlock {
        val attrRef = readAttrsRef(node.nodeRef)
        require(attrRef != null)
        return readAttrs(attrsRef = attrRef)
    }

    private fun createAttrs(attrs: AttributesBlock): BlockStart {
        val attrsSegment: MemoryBlock = blockStore.reserveSegment()
        val attrsByteChannel = attrsSegment.newByteChannel(mode = MemoryOpenOptions.WRITE, lock = null)
        attrsByteChannel.use {
            it.writeInstant(attrs.lastAccessTime)
            it.writeInstant(attrs.lastModifiedTime)
            it.writeInstant(attrs.creationTime)
            it.writeString(PosixFilePermissions.toString(attrs.permissions))
            it.writeString(attrs.owner)
            it.writeString(attrs.group)
        }
        return attrsSegment
    }

    fun updateFileTime(nodeRef: BlockStart, attrs: AttributesBlock) {
        blockStore.lock(nodeRef).use(MemoryOpenOptions.WRITE) {
            val attrsRef = readAttrsRef(nodeRef)
            require(attrsRef != null)
            val attrsSegment = blockStore.findSegment(attrsRef)
            attrsSegment.use {
                val attrsByteChannel = attrsSegment.newByteChannel(mode = MemoryOpenOptions.WRITE, lock = null)
                attrsByteChannel.use {
                    it.writeInstant(attrs.lastAccessTime)
                    it.writeInstant(attrs.lastModifiedTime)
                    it.readInstant()
                    it.skipRemaining()
                }
            }
        }
    }

    fun updatePermissions(nodeRef: BlockStart, attrs: AttributesBlock) {
        blockStore.lock(nodeRef).use(MemoryOpenOptions.WRITE) {
            val attrsRef = readAttrsRef(nodeRef)
            require(attrsRef != null)
            val attrsSegment = blockStore.findSegment(attrsRef)
            attrsSegment.use {
                val attrsByteChannel = attrsSegment.newByteChannel(mode = MemoryOpenOptions.WRITE, lock = null)
                attrsByteChannel.use {
                    it.readInstant()
                    it.readInstant()
                    it.readInstant()
                    it.writeString(PosixFilePermissions.toString(attrs.permissions))
                    it.skipRemaining()
                }
            }
        }
    }

    private fun readAttrs(attrsRef: BlockStart): AttributesBlock {
        val attrsNode = blockStore.findSegment(attrsRef)
        attrsNode.use {
            val attrsByteChannel = attrsNode.newByteChannel(mode = MemoryOpenOptions.READ, lock = null)
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
    }

    private fun readAttrsRef(nodeRef: BlockStart): BlockStart? {
        val read = read(type = ValidNode::class, nodeRef = nodeRef)
        val attrRef = read.attrsRef
        return if (attrRef.isValid()) {
            attrRef
        } else {
            null
        }
    }

    //    children
    fun findChildren(parent: DirectoryNode): Sequence<ValidNode> {
        return readChildIds(parent).map { read(type = ValidNode::class, nodeRef = it) }
    }

    fun findChildByName(parent: DirectoryNode?, name: String): ValidNode? {
        if (parent == null) {
            return null
        }
        val findChildIds = readChildIds(parent)
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
        val children: Sequence<BlockStart> = readChildIds(parent) + childRef
        upsertChildren(nodeRef = parent.nodeRef, prevDataRef = parent.dataRef, children = children)
    }

    private fun readChildIds(parent: DirectoryNode): Sequence<BlockStart> {
        val dataRef = readDataRef(parent.nodeRef) ?: return sequenceOf()
        return readChildIds(dataRef)
    }

    private fun removeChildByName(parent: DirectoryNode, name: String) {
        val childIds = readChildIds(parent)
        val findChildByName = findChildByName(childIds, name)
        require(findChildByName != null)

        val children = mutableListOf<BlockStart>()
        children.addAll(childIds)
        children.remove(findChildByName.nodeRef)
        upsertChildren(nodeRef = parent.nodeRef, prevDataRef = parent.dataRef, children = children.asSequence())
    }

    private fun upsertChildren(nodeRef: BlockStart, prevDataRef: BlockStart, children: Sequence<BlockStart>) {
        if (prevDataRef.isValid()) {
            val oldDataSegment = blockStore.findSegment(prevDataRef)
            oldDataSegment.use {
                blockStore.releaseAll(oldDataSegment)
            }
        }

        val newDataRef = if (children.count() > 0) {
            val newDataSegment: MemoryBlock = blockStore.reserveSegment()
            val newDataByteChannel = newDataSegment.newByteChannel(mode = MemoryOpenOptions.WRITE, lock = null)
            newDataByteChannel.use {
                it.writeRefs(children)
            }
            newDataSegment
        } else {
            BlockStart.Invalid
        }
        updateDataRef(nodeRef, newDataRef)
    }

    private fun readChildIds(dataRef: BlockStart): Sequence<BlockStart> {
        val dataNode = blockStore.findSegment(dataRef)
        dataNode.use {
            val dataByteChannel = dataNode.newByteChannel(mode = MemoryOpenOptions.READ, lock = null)
            dataByteChannel.use {
                return it.readRefs()
            }
        }
    }
}