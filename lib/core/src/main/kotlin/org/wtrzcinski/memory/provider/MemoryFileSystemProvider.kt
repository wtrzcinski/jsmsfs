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

package org.wtrzcinski.memory.provider

import org.wtrzcinski.memory.MemorySegmentContext
import org.wtrzcinski.memory.MemorySegmentFileSystem
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.allocator.MemoryScopeType
import org.wtrzcinski.memory.buffer.AbstractMemoryReadWriteBuffer
import org.wtrzcinski.memory.exception.MemoryUnsupportedOperationException
import org.wtrzcinski.memory.mapper.NodeType
import org.wtrzcinski.memory.mapper.NodeType.Companion.Directory
import org.wtrzcinski.memory.mapper.NodeType.Companion.SymbolicLink
import org.wtrzcinski.memory.mode.ModeMonitor
import org.wtrzcinski.memory.path.HardFilePath
import org.wtrzcinski.memory.path.RealFilePath
import org.wtrzcinski.memory.provider.MemoryFileOpenOptions.Companion.REQUIRE_NEW
import org.wtrzcinski.memory.provider.attribute.MemoryFileAttributeView
import org.wtrzcinski.memory.provider.attribute.MemoryFileAttributes
import org.wtrzcinski.memory.provider.channel.MemoryAsynchronousFileChannel
import org.wtrzcinski.memory.provider.channel.MemoryFileChannel
import org.wtrzcinski.memory.provider.directory.MemorySecureDirectoryStream
import org.wtrzcinski.memory.util.Check
import org.wtrzcinski.memory.util.IOUtil
import org.wtrzcinski.memory.util.Require
import java.lang.AutoCloseable
import java.net.URI
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.FileChannel
import java.nio.file.*
import java.nio.file.attribute.*
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.use

@OptIn(ExperimentalAtomicApi::class)
@Suppress("UNCHECKED_CAST", "UsePropertyAccessSyntax")
class MemoryFileSystemProvider(
    val fileSystem: MemorySegmentFileSystem? = null,
) : FileSystemProvider(), AutoCloseable {

    companion object {
        const val schemaName: String = "jsmsfs"
        const val Scope = "scope"
        const val Capacity = "capacity"
        const val MaxBlockSize = "maxBlockSize"

        private val filesystems = ConcurrentHashMap<String, MemoryFileSystem>()

        @Synchronized
        fun newFileSystem(uri: URI, env: Map<String, *>): MemoryFileSystem {
            synchronized(filesystems) {
                val capacity: DefaultBlockSize = DefaultBlockSize.readSize(env[Capacity]) ?: throw IllegalArgumentException("Missing capacity parameter")
                val blockSize: DefaultBlockSize = DefaultBlockSize.readSize(env[MaxBlockSize]?.toString()) ?: MemorySegmentContext.DefaultMaxBlockSize
                val scope: MemoryScopeType = env[Scope]?.toString()?.uppercase()?.let { MemoryScopeType.valueOf(it) } ?: MemoryScopeType.DEFAULT

                val rawQuery = uri.rawQuery ?: ""
                val context = MemorySegmentContext(
                    scope = scope,
                    capacity = capacity,
                    blockSize = blockSize,
                    env = env,
                )
                val fileSystem = MemorySegmentFileSystem(
                    name = rawQuery,
                    context = context,
                )
                val javaFileSystem = MemoryFileSystem(
                    env = env,
                    provider = MemoryFileSystemProvider(fileSystem = fileSystem)
                )
                filesystems[rawQuery] = javaFileSystem
                return javaFileSystem
            }
        }

        fun getFileSystem(uri: URI): MemoryFileSystem {
            synchronized(filesystems) {
                val rawQuery = uri.rawQuery ?: ""
                val system = filesystems[rawQuery]
                return system ?: throw FileSystemNotFoundException()
            }
        }
    }

    private val monitor = ModeMonitor()

    override fun close() {
        if (monitor.tryClose()) {
            fileSystem?.close()
        }
    }

    override fun getScheme(): String {
        return schemaName
    }

    override fun newFileSystem(uri: URI, env: Map<String, *>): MemoryFileSystem {
        return Companion.newFileSystem(uri, env)
    }

    override fun getFileSystem(uri: URI): MemoryFileSystem {
        return Companion.getFileSystem(uri)
    }

    override fun newByteChannel(
        child: Path,
        options: Set<OpenOption>,
        vararg attrs: FileAttribute<*>
    ): AbstractMemoryReadWriteBuffer {
        require(child is MemoryFilePathAdapter)
        checkNotNull(fileSystem)

        val parent = child.parent?.toRealPath()
        val parentDelegate = parent?.delegate
        require(parentDelegate is RealFilePath?)
        val parentNode = parentDelegate?.node
        requireNotNull(parentNode)
        require(parent.delegate.type == Directory)

        val mode = MemoryFileOpenOptions(options as Set<StandardOpenOption>)
        return fileSystem.getOrCreateData(parent = parentNode, childName = child.delegate.name, mode = mode)
    }

    override fun newFileChannel(path: Path, options: Set<OpenOption>, vararg attrs: FileAttribute<*>): FileChannel {
        return MemoryFileChannel(delegate = newByteChannel(path, options, *attrs))
    }

    override fun newAsynchronousFileChannel(path: Path, options: Set<OpenOption>, executor: ExecutorService, vararg attrs: FileAttribute<*>): AsynchronousFileChannel {
        return MemoryAsynchronousFileChannel()
    }

    override fun createDirectory(path: Path, vararg attrs: FileAttribute<*>) {
        require(path is MemoryFilePathAdapter)
        checkNotNull(fileSystem)

        val parentPath = path.parent
        if (parentPath != null) {
            createDirectory(parentPath, *attrs)
        }

        val delegate1 = parentPath?.toRealPath()?.delegate as RealFilePath?
        val parentNode = delegate1?.node
        require(parentNode == null || parentNode.readType() == Directory)

        if (!path.exists()) {
            Check.isNotNull { parentNode }

            fileSystem.getOrCreateFile(
                parent = parentNode,
                childType = Directory,
                childName = path.delegate.name,
                mode = REQUIRE_NEW,
                targetNode = null,
            )

            Check.isTrue {
                val delegate = path.toRealPath().delegate as RealFilePath
                delegate.node.readType() == Directory
            }
        }
    }

    override fun createLink(link: Path, existing: Path) {
        TODO("Not yet implemented")
    }

    override fun createSymbolicLink(path: Path, target: Path, vararg attrs: FileAttribute<*>) {
        require(path is MemoryFilePathAdapter)
        require(target is MemoryFilePathAdapter)
        checkNotNull(fileSystem)

        val parentPath = path.parent
        if (parentPath != null) {
            createDirectory(parentPath, *attrs)
        }

        val target = target.toRealPath()

        val delegate1 = parentPath?.toRealPath()?.delegate as RealFilePath?
        val parentNode = delegate1?.node
        require(parentNode?.readType() == Directory)

        val delegate = target.delegate as RealFilePath
        val targetNode = delegate.node
        require(targetNode.readType() == NodeType.Regular || targetNode.readType() == Directory)

        if (!path.exists()) {
            Check.isNotNull { parentNode }

            fileSystem.getOrCreateFile(
                parent = parentNode,
                childType = SymbolicLink,
                childName = path.delegate.name,
                targetNode = target.toUri(),
                mode = REQUIRE_NEW,
            )

            Check.isTrue {
                val node = path.toRealPath().delegate as RealFilePath
                node.node.readType() == SymbolicLink }
        }
    }

    override fun move(source: Path, target: Path, vararg options: CopyOption) {
        require(source is MemoryFilePathAdapter)
        require(target is MemoryFilePathAdapter)

        copy(source, target, *options)

        delete(source)
    }

    override fun copy(source: Path?, target: Path?, vararg options: CopyOption) {
        require(source is MemoryFilePathAdapter)
        require(target is MemoryFilePathAdapter)

        val sourceByteBuffer = Files.newByteChannel(source, StandardOpenOption.READ)
        val targetByteBuffer = Files.newByteChannel(target, StandardOpenOption.WRITE, StandardOpenOption.CREATE)

        sourceByteBuffer.use {
            targetByteBuffer.use {
                IOUtil.transfer(sourceByteBuffer, targetByteBuffer)
            }
        }
    }

    override fun delete(path: Path) {
        require(path is MemoryFilePathAdapter)
        checkNotNull(fileSystem)

        val realPath = path.toRealPath()
        val parent = realPath.parent
        if (parent != null) {
            parent.delegate as RealFilePath
            realPath.delegate as RealFilePath
            this.fileSystem.delete(parent.delegate.node, realPath.delegate.node)
        }
    }

    override fun <A : BasicFileAttributes> readAttributes(path: Path, type: Class<A>, vararg options: LinkOption): A {
        if (PosixFileAttributes::class.java == type) {
            require(path is MemoryFilePathAdapter)
            checkNotNull(fileSystem)
            val realPath = path.toRealPath()
            val delegate = realPath.delegate as RealFilePath
            val node = delegate.node
            val attrs = node.readAttrs()
            return type.cast(
                MemoryFileAttributes(
                    fileSystem = fileSystem,
                    name = "posix",
                    node = node,
                    attrs = attrs,
                )
            )
        } else if (BasicFileAttributes::class.java == type) {
            require(path is MemoryFilePathAdapter)
            checkNotNull(fileSystem)
            val realPath = path.toRealPath()
            val delegate = realPath.delegate as RealFilePath
            val node = delegate.node
            val attrs = node.readAttrs()
            return type.cast(
                MemoryFileAttributes(
                    fileSystem = fileSystem,
                    name = "basic",
                    node = node,
                    attrs = attrs,
                )
            )
        }
        Require.unsupported()
    }

    override fun <V : FileAttributeView?> getFileAttributeView(
        path: Path,
        type: Class<V>,
        vararg options: LinkOption
    ): V {
        if (PosixFileAttributeView::class.java == type) {
            require(path is MemoryFilePathAdapter)
            checkNotNull(fileSystem)
            val realPath = path.toRealPath()
            val delegate = realPath.delegate as RealFilePath
            val pathNode = delegate.node

            return type.cast(
                MemoryFileAttributeView(
                    fileSystem = fileSystem,
                    name = "posix",
                    node = pathNode,
                )
            )
        } else if (BasicFileAttributeView::class.java == type) {
            require(path is MemoryFilePathAdapter)
            checkNotNull(fileSystem)
            val realPath = path.toRealPath()
            val delegate = realPath.delegate as RealFilePath
            val pathNode = delegate.node

            return type.cast(
                MemoryFileAttributeView(
                    fileSystem = fileSystem,
                    name = "basic",
                    node = pathNode,
                )
            )
        }
        throw MemoryUnsupportedOperationException()
    }

    override fun checkAccess(path: Path, vararg modes: AccessMode) {
        require(path is MemoryFilePathAdapter)

        if (!path.exists()) {
            throw NoSuchFileException(toString())
        }
    }

    override fun readAttributes(path: Path, attributes: String, vararg options: LinkOption): Map<String?, Any?> {
        require(attributes == "*")

        val attrs = readAttributes(path, PosixFileAttributes::class.java, *options)
        return mapOf(
            MemoryFileAttributes::fileKey.name to attrs.fileKey(),
            MemoryFileAttributes::isRegularFile.name to attrs.isRegularFile(),
            MemoryFileAttributes::isDirectory.name to attrs.isDirectory(),
            MemoryFileAttributes::isSymbolicLink.name to attrs.isSymbolicLink(),
            MemoryFileAttributes::isOther.name to attrs.isOther(),
            MemoryFileAttributes::lastAccessTime.name to attrs.lastAccessTime(),
            MemoryFileAttributes::lastModifiedTime.name to attrs.lastModifiedTime(),
            MemoryFileAttributes::creationTime.name to attrs.creationTime(),
            MemoryFileAttributes::permissions.name to attrs.permissions(),
        )
    }

    override fun setAttribute(path: Path?, attribute: String?, value: Any?, vararg options: LinkOption?) {
        TODO("Not yet implemented")
    }

    override fun isSameFile(path1: Path, path2: Path): Boolean {
        require(path1 is MemoryFilePathAdapter)
        require(path2 is MemoryFilePathAdapter)
        require(path1.delegate is HardFilePath)
        require(path2.delegate is HardFilePath)

        val path1Node = path1.delegate.node
        val path2Node = path2.delegate.node
        return path1Node == path2Node
    }

    override fun newDirectoryStream(parent: Path, filter: DirectoryStream.Filter<in Path>): DirectoryStream<Path> {
        require(parent is MemoryFilePathAdapter)
        require(parent.delegate is RealFilePath)

        Check.isTrue { parent.delegate.node.readType() == Directory }

        return MemorySecureDirectoryStream(parent = parent, filter = filter)
    }

    override fun getPath(uri: URI): Path {
        val fileSystem = getFileSystem(uri)
        return fileSystem.getPath(uri.path)
    }

    override fun getFileStore(path: Path?): FileStore {
        checkNotNull(fileSystem)

        return MemoryFileStore(fileSystem.context.ledger)
    }

    override fun isHidden(path: Path): Boolean {
        require(path is MemoryFilePathAdapter)

        return path.delegate.isHidden()
    }
}