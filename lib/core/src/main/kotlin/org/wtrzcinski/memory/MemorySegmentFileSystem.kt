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

package org.wtrzcinski.memory

import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.buffer.AbstractMemoryReadWriteBuffer
import org.wtrzcinski.memory.lock.MemoryFileLock
import org.wtrzcinski.memory.lock.MemoryFileLock.Companion.use
import org.wtrzcinski.memory.mapper.NodeMapper
import org.wtrzcinski.memory.mapper.NodeType
import org.wtrzcinski.memory.mode.*
import org.wtrzcinski.memory.mode.OpenMode.Get
import org.wtrzcinski.memory.mode.OpenMode.Put
import org.wtrzcinski.memory.path.HardFilePath
import org.wtrzcinski.memory.path.RootFilePath
import org.wtrzcinski.memory.provider.MemoryFileOpenOptions
import org.wtrzcinski.memory.provider.MemoryFileOpenOptions.Companion.WRITE_TRUNCATE
import org.wtrzcinski.memory.util.Check
import org.wtrzcinski.memory.util.Require
import java.io.File
import java.net.URI
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.NoSuchFileException
import java.nio.file.attribute.PosixFilePermission
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class MemorySegmentFileSystem(
    val name: String,
    val context: MemorySegmentContext,
) : AutoCloseable, ModeMonitor(
    mode = if (context.ledger.memory.isReadOnly) {
        Mode.read()
    } else {
        Mode(
            OpenMode.Post,
            UnsafeMode.RequireNew,
            SafeMode.All
        )
    }
) {

    val rootRef: NodeMapper = getOrCreateFile(
        parent = null,
        childType = NodeType.Directory,
        childName = File.separator,
        mode = MemoryFileOpenOptions.REQUIRE_NEW,
        targetNode = null,
    )

    val root: RootFilePath = RootFilePath(
        fileSystem = this,
        ref = rootRef.ref(),
        node = rootRef,
    )

    fun isAlive(): Boolean {
        return context.ledger.memory.scope().isAlive
    }

    override fun close() {
        if (tryClose()) {
            context.close()
        }
    }

    fun getOrCreateData(parent: NodeMapper, childName: String, mode: MemoryFileOpenOptions): AbstractMemoryReadWriteBuffer {
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
            val string = context.ledger.existingChannel(
                childName, readDataRef, lock = MemoryFileLock.unlocked(
                    Mode.read()
                )
            )
            val uri = string.use {
                val string = context.schemas.stringHandler.read(it)
                URI(string)
            }
            val resolve = root.resolve(uri.path)
            if (resolve !is HardFilePath) {
                throw NoSuchFileException(childName)
            }
            val resolveParent = resolve.parent
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

    fun getOrCreateFile(
        childType: NodeType,
        parent: NodeMapper?,
        childName: String,
        mode: MemoryFileOpenOptions,
        targetNode: URI?
    ): NodeMapper {
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

    private fun createFile(
        childType: NodeType,
        name: String,
        target: URI? = null,
        parent: NodeMapper? = null
    ): NodeMapper {
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

        nodeMapper.writeLinkCount(DefaultBlockSize(1))
        if (target != null) {
            val symbolicLink = this.context.mappers.createString(target.toString())
            symbolicLink.flip(Get)
            symbolicLink.close()
            nodeMapper.writeDataRef(symbolicLink.address())
        } else {
            nodeMapper.writeDataRef(BlockAddress.InvalidAddress)
        }
        nodeMapper.writeAttrsRef(attrsMapper.ref())
        nodeMapper.writeNameRef(nameMapper.address())
        nodeMapper.flip(Put)

        if (parent != null) {
            val parentLock = context.locks.newLock(ref = parent.ref(), mode = WRITE_TRUNCATE)
            parentLock.use {
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

        val locks = context.locks
        val ledger = context.ledger

        val parentLock = locks.newLock(ref = parent.ref(), mode = WRITE_TRUNCATE)
        parentLock.use {
            parent.removeChild(child)
        }

        val childLock = locks.newLock(ref = child.ref(), mode = WRITE_TRUNCATE)
        val releaseResult = childLock.use {
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
                    ledger.releaseAll(dataRef)
                }
            }
            var releaseResult = ledger.releaseAll(child.readAttrsRef())
            releaseResult += ledger.releaseAll(child.readNameRef())
            releaseResult += ledger.releaseAll(child.ref())
            releaseResult
        }

        for (pair in releaseResult.neighbors) {
            val first = pair.first
            val second = pair.second

            checkNotNull(second.first)

//            todo wojtek
//            val lock = locks.newLock(ref = checkNotNull(second.first), mode = WRITE_TRUNCATE)
//            lock.use {
//                val existingBuffer = ledger.existingBuffer(second)
//                existingBuffer.body
//                ledger.releaseOne(second)
//            }
        }
    }

    fun updateFileTime(node: NodeMapper, lastAccessTime: Instant, lastModifiedTime: Instant) {
        context.locks.newLock(ref = node.ref(), mode = WRITE_TRUNCATE).use {
            val mode1 = Mode.update()
            val lock = MemoryFileLock.unlocked(mode1)
            val attrsRef = node.readAttrsRef()
            val ledger = context.ledger
            val attrsByteChannel = ledger.existingChannel(ref = attrsRef, lock = lock)
            val schemas = context.schemas
            attrsByteChannel.use {
                schemas.instantHandler.write(it, lastAccessTime)
                schemas.instantHandler.write(it, lastModifiedTime)
                it.skipRemaining()
            }
        }
    }

    fun updatePermissions(node: NodeMapper, permissions: Set<PosixFilePermission>) {
        context.locks.newLock(ref = node.ref(), mode = WRITE_TRUNCATE).use {
            val mode1 = Mode.update()
            val lock = MemoryFileLock.unlocked(mode1)
            val attrsRef = node.readAttrsRef()
            val ledger = context.ledger
            val attrsByteChannel = ledger.existingChannel(ref = attrsRef, lock = lock)
            val schemas = context.schemas
            attrsByteChannel.use {
                schemas.instantHandler.read(it)
                schemas.instantHandler.read(it)
                schemas.instantHandler.read(it)
                schemas.permissionsHandler.write(buffer = it, value = permissions)
                it.skipRemaining()
            }
        }
    }
}