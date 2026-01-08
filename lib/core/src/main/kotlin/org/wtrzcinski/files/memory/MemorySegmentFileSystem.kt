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

package org.wtrzcinski.files.memory

import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.lock.MemoryFileLock.Companion.use
import org.wtrzcinski.files.memory.mapper.NodeMapper
import org.wtrzcinski.files.memory.mapper.NodeType
import org.wtrzcinski.files.memory.mode.*
import org.wtrzcinski.files.memory.mode.OpenMode.Get
import org.wtrzcinski.files.memory.mode.OpenMode.Put
import org.wtrzcinski.files.memory.path.FilePath
import org.wtrzcinski.files.memory.path.HardFilePath
import org.wtrzcinski.files.memory.provider.MemoryFileOpenOptions
import org.wtrzcinski.files.memory.provider.MemoryFileOpenOptions.Companion.WRITE_TRUNCATE
import org.wtrzcinski.files.memory.util.Check
import org.wtrzcinski.files.memory.util.Require
import java.io.File
import java.net.URI
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.NoSuchFileException
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions.toString
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class MemorySegmentFileSystem(
    val context: MemorySegmentContext,
) : AutoCloseable, ModeMonitor(
    mode = if (context.ledger.memory.isReadOnly()) {
        Mode.read()
    } else {
        Mode(OpenMode.Post, WriteMode.RequireNew, ReadMode.Block)
    }
) {

    val rootRef: NodeMapper = getOrCreateFile(
        parent = null,
        childType = NodeType.Directory,
        childName = File.separator,
        mode = MemoryFileOpenOptions.REQUIRE_NEW,
        targetNode = null,
    )

    val root: HardFilePath = HardFilePath(
        fileSystem = this,
        parent = null,
        ref = rootRef.ref(),
        node = rootRef,
    )

    fun isAlive(): Boolean {
        return context.ledger.memory.scope().isAlive()
    }

    override fun close() {
        if (tryClose()) {
            context.close()
        }
    }

    fun getOrCreateData(parent: NodeMapper, childName: String, mode: MemoryFileOpenOptions): MemoryReadWriteBuffer {
        throwIfNotWritable()

        val child = getOrCreateFile(
            parent = parent,
            childName = childName,
            childType = NodeType.Regular,
            mode = mode,
            targetNode = null,
        )

        if (child.readType() == NodeType.SymbolicLink) {
            val readDataRef = checkNotNull(child.readDataRef())
            val string = context.ledger.existingChannel(childName, readDataRef, lock = MemoryFileLock.unlocked(Mode.read()))
            val uri = string.use {
                it.readUri()
            }
            val resolve = FilePath.resolve(root, uri.path)
            if (resolve !is HardFilePath) {
                throw NoSuchFileException(childName)
            }
            val resolveParent = resolve.parent as HardFilePath
            return getOrCreateData(parent = resolveParent.node, childName = resolve.name, mode = mode)
        }

        var dataRef: BlockAddress? = child.readDataRef()

        val childLock = context.locks.newLock(ref = child.ref(), mode = mode)
        childLock.acquire()
        try {
            if (dataRef == null) {
                dataRef = child.readDataRef()
                if (dataRef == null) {
                    Check.isTrue { mode.readWrite }

                    val dataSegment = context.ledger.allocateChannel(lock = childLock)
                    child.writeDataRef(ref = dataSegment.address())
                    return dataSegment
                } else {
                    return context.ledger.existingChannel(name = childName, ref = dataRef, lock = childLock)
                }
            } else {
                return context.ledger.existingChannel(name = childName, ref = dataRef, lock = childLock)
            }
        } catch (e: Exception) {
            childLock.release()

            throw e
        }
    }

    fun getOrCreateFile(childType: NodeType, parent: NodeMapper?, childName: String, mode: MemoryFileOpenOptions, targetNode: URI?): NodeMapper {
        throwIfNotWritable()

        Require.notEmpty(childName)
        require(targetNode == null || childType == NodeType.SymbolicLink)

        val existingChild = parent?.findChildByName(childName)
        if (existingChild != null) {
            if (mode.requireNew) {
                throw FileAlreadyExistsException(childName)
            }
            return existingChild
        }

        val parentLock = context.locks.newLock(ref = parent?.ref(), mode = WRITE_TRUNCATE)
        return parentLock.use {
            val existingChild = parent?.findChildByName(childName)
            if (existingChild != null) {
                if (mode.requireNew) {
                    throw FileAlreadyExistsException(childName)
                }
                return@use existingChild
            }
            if (!mode.create) {
                throw NoSuchFileException(childName)
            }

            return@use createFile(childType, childName, targetNode, parent)
        }
    }

    private fun createFile(childType: NodeType, name: String, target: URI? = null, parent: NodeMapper? = null): NodeMapper {
        Require.notEmpty(name)

        if (parent?.findChildByName(name) != null) {
            throw FileAlreadyExistsException(name)
        }

        val nameMapper = this.context.mappers.createString(name)
        nameMapper.flip(Get)
        nameMapper.close()

        val attrsMapper = this.context.mappers.createAttrs()
        attrsMapper.flip(Get)
        attrsMapper.close()

        val nodeMapper = this.context.mappers.createNode()
        if (target != null) {
            nodeMapper.writeType(NodeType.SymbolicLink)
        } else {
            nodeMapper.writeType(childType)
        }

        nodeMapper.writeLinkCount(1)
        if (target != null) {
            val symbolicLink = this.context.mappers.createString(target.toString())
            symbolicLink.flip(Get)
            symbolicLink.close()
            nodeMapper.writeDataRef(symbolicLink.address())
        } else {
            nodeMapper.writeDataRef(BlockAddress.InvalidOffset)
        }
        nodeMapper.writeAttrsRef(attrsMapper.ref())
        nodeMapper.writeNameRef(nameMapper.address())
        nodeMapper.flip(Put)

        if (parent != null) {
            context.locks.newLock(ref = parent.ref(), mode = WRITE_TRUNCATE).use {
                parent.addChild(nodeMapper)
            }
        }

        return nodeMapper
    }

    fun read(ref: BlockAddress): NodeMapper {
        return this.context.mappers.readNode(ref)
    }

    fun delete(parent: NodeMapper, child: NodeMapper) {
        Check.isTrue { child.ref() != this.rootRef }
        require(parent.readType() == NodeType.Directory)

        val parentLock = context.locks.newLock(ref = parent.ref(), mode = WRITE_TRUNCATE)
        parentLock.use {
            parent.removeChild(child)
        }

        val childLock = context.locks.newLock(ref = child.ref(), mode = WRITE_TRUNCATE)
        childLock.use {
            val readType = child.readType()
            if (readType == NodeType.Directory) {
                val hasChildren = child.hasChildren()
                if (hasChildren) {
                    throw DirectoryNotEmptyException(child.readName())
                }
            }
            if (readType == NodeType.Regular || readType == NodeType.Directory || readType == NodeType.SymbolicLink) {
                val dataRef = child.readDataRef()
                if (dataRef != null) {
                    context.ledger.releaseAll(dataRef)
                }
            }
            context.ledger.releaseAll(child.readAttrsRef())
            context.ledger.releaseAll(child.readNameRef())
            context.ledger.releaseAll(child.ref())
        }
    }

    fun updateFileTime(node: NodeMapper, lastAccessTime: Instant, lastModifiedTime: Instant) {
        context.locks.newLock(ref = node.ref(), mode = WRITE_TRUNCATE).use {
            val mode1 = Mode.update()
            val lock = MemoryFileLock.unlocked(mode1)
            val attrsRef = node.readAttrsRef()
            val attrsByteChannel = context.ledger.existingChannel(name = "", ref = attrsRef, lock = lock)
            attrsByteChannel.use {
                it.writeInstant(lastAccessTime)
                it.writeInstant(lastModifiedTime)
                it.skipRemaining()
            }
        }
    }

    fun updatePermissions(node: NodeMapper, permissions: Set<PosixFilePermission>) {
        context.locks.newLock(ref = node.ref(), mode = WRITE_TRUNCATE).use {
            val mode1 = Mode.update()
            val lock = MemoryFileLock.unlocked(mode1)
            val attrsRef = node.readAttrsRef()
            val attrsByteChannel = context.ledger.existingChannel(name = "", ref = attrsRef, lock = lock)
            attrsByteChannel.use {
                it.readInstant()
                it.readInstant()
                it.readInstant()
                it.writeString(toString(permissions))
                it.skipRemaining()
            }
        }
    }
}