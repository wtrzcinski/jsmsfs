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

package org.wtrzcinski.files.memory.provider

import org.wtrzcinski.files.memory.MemorySegmentContext
import org.wtrzcinski.files.memory.MemorySegmentFileSystem
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.allocator.MemoryScopeType
import org.wtrzcinski.files.memory.buffer.channel.FragmentedReadWriteBuffer
import org.wtrzcinski.files.memory.exception.MemoryUnsupportedOperationException
import org.wtrzcinski.files.memory.node.DirectoryNode
import org.wtrzcinski.files.memory.node.NodeType.Directory
import org.wtrzcinski.files.memory.node.NodeType.SymbolicLink
import org.wtrzcinski.files.memory.node.RegularFileNode
import org.wtrzcinski.files.memory.node.SymbolicLinkNode
import org.wtrzcinski.files.memory.path.AbstractFilePath
import org.wtrzcinski.files.memory.path.HardFilePath
import org.wtrzcinski.files.memory.provider.MemoryFileOpenOptions.Companion.REQUIRE_NEW
import org.wtrzcinski.files.memory.util.Check
import org.wtrzcinski.files.memory.util.IOUtil
import org.wtrzcinski.files.memory.util.Require
import java.lang.AutoCloseable
import java.net.URI
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.FileChannel
import java.nio.file.*
import java.nio.file.attribute.*
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.use

@OptIn(ExperimentalAtomicApi::class)
@Suppress("UNCHECKED_CAST", "UsePropertyAccessSyntax")
class MemoryFileSystemProvider(
    val fileSystem: MemorySegmentFileSystem? = null,
) : FileSystemProvider(), AutoCloseable {

    companion object {
        private val filesystems = ConcurrentHashMap<String, MemoryFileSystem>()

        @Synchronized
        fun newFileSystem(uri: URI, env: Map<String, *>): MemoryFileSystem {
            synchronized(filesystems) {
                val capacity: ByteSize = ByteSize.readSize(env["capacity"]) ?: throw IllegalArgumentException("Missing capacity parameter")
                val blockSize: ByteSize = ByteSize.readSize(env["blockSize"]?.toString()) ?: ByteSize(1024 * 4)
                val scope: MemoryScopeType = env["scope"]?.toString()?.uppercase()?.let { MemoryScopeType.valueOf(it) } ?: MemoryScopeType.DEFAULT

                val rawQuery = uri.rawQuery ?: ""
                val context = MemorySegmentContext(
                    scope = scope,
                    capacity = capacity,
                    blockSize = blockSize,
                    env = env,
                )
                val fileSystem = MemorySegmentFileSystem(
                    context = context,
                    name = rawQuery,
                )
                val javaFileSystem = MemoryFileSystem(
                    env = env,
                    name = rawQuery,
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

    private val closed = AtomicBoolean(false)

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            fileSystem?.close()
        }
    }

    override fun getScheme(): String {
        return "jsmsfs"
    }

    override fun newFileSystem(uri: URI, env: Map<String, *>): MemoryFileSystem {
        return Companion.newFileSystem(uri, env)
    }

    override fun getFileSystem(uri: URI): MemoryFileSystem {
        return Companion.getFileSystem(uri)
    }

    override fun newByteChannel(child: Path, options: Set<OpenOption>, vararg attrs: FileAttribute<*>): FragmentedReadWriteBuffer {
        require(child is AbstractFilePath)
        checkNotNull(fileSystem)

        val parent = child.parent?.toRealPath()
        val parentNode = parent?.node
        require(parentNode is DirectoryNode)

        val mode = MemoryFileOpenOptions(options as Set<StandardOpenOption>)
        return fileSystem.getOrCreateData(parent = parentNode, childName = child.name, mode = mode)
    }

    override fun newFileChannel(path: Path, options: Set<OpenOption>, vararg attrs: FileAttribute<*>): FileChannel {
        return MemoryFileChannel(delegate = newByteChannel(path, options, *attrs))
    }

    override fun newAsynchronousFileChannel(path: Path?, options: Set<OpenOption>, executor: ExecutorService, vararg attrs: FileAttribute<*>): AsynchronousFileChannel? {
        Require.todo()
    }

    override fun createDirectory(path: Path, vararg attrs: FileAttribute<*>) {
        require(path is AbstractFilePath)
        checkNotNull(fileSystem)

        val parentPath = path.parent
        if (parentPath != null) {
            createDirectory(parentPath, *attrs)
        }

        val parentNode = parentPath?.toRealPath()?.node
        require(parentNode is DirectoryNode?)

        if (!path.exists()) {
            Check.isNotNull { parentNode }

            fileSystem.getOrCreateFile(
                parent = parentNode,
                childType = Directory,
                childName = path.name,
                mode = REQUIRE_NEW,
            )

            Check.isTrue { path.toRealPath().node is DirectoryNode }
        }
    }

    override fun createLink(link: Path?, existing: Path?) {
        super.createLink(link, existing)
    }

    override fun createSymbolicLink(path: Path, target: Path, vararg attrs: FileAttribute<*>) {
        require(path is AbstractFilePath)
        require(target is AbstractFilePath)
        checkNotNull(fileSystem)

        val parentPath = path.parent
        if (parentPath != null) {
            createDirectory(parentPath, *attrs)
        }

        val target = target.toRealPath()

        val parentNode = parentPath?.toRealPath()?.node
        require(parentNode is DirectoryNode?)

        val targetNode = target.node
        require(targetNode is RegularFileNode || targetNode is DirectoryNode)

        if (!path.exists()) {
            Check.isNotNull { parentNode }

            fileSystem.getOrCreateFile(
                parent = parentNode,
                childType = SymbolicLink,
                childName = path.name,
                targetNode = targetNode,
                mode = REQUIRE_NEW
            )

            Check.isTrue { path.toRealPath().node is SymbolicLinkNode }
        }
    }

    override fun move(source: Path, target: Path, vararg options: CopyOption) {
        require(source is AbstractFilePath)
        require(target is AbstractFilePath)

        copy(source, target, *options)

        delete(source)
    }

    override fun copy(source: Path?, target: Path?, vararg options: CopyOption) {
        require(source is AbstractFilePath)
        require(target is AbstractFilePath)

        val sourceByteBuffer = Files.newByteChannel(source, StandardOpenOption.READ)
        val targetByteBuffer = Files.newByteChannel(target, StandardOpenOption.WRITE, StandardOpenOption.CREATE)

        sourceByteBuffer.use {
            targetByteBuffer.use {
                IOUtil.transfer(sourceByteBuffer, targetByteBuffer)
            }
        }
    }

    override fun delete(path: Path) {
        require(path is AbstractFilePath)
        checkNotNull(fileSystem)

        val realPath = path.toRealPath()
        val parent = realPath.parent
        if (parent != null) {
            this.fileSystem.delete(parent.node, realPath.node)
        }
    }

    override fun <A : BasicFileAttributes> readAttributes(path: Path, type: Class<A>, vararg options: LinkOption): A {
        if (PosixFileAttributes::class.java == type) {
            require(path is AbstractFilePath)
            checkNotNull(fileSystem)
            val realPath = path.toRealPath()
            val node = realPath.node
            val attrs = fileSystem.findAttrs(realPath.nodeRef)
            return type.cast(
                MemoryFileAttributes(
                    fileSystem = fileSystem,
                    name = "posix",
                    node = node,
                    attrs = attrs,
                )
            )
        } else if (BasicFileAttributes::class.java == type) {
            require(path is AbstractFilePath)
            checkNotNull(fileSystem)
            val realPath = path.toRealPath()
            val node = realPath.node
            val attrs = fileSystem.findAttrs(realPath.nodeRef)
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
            require(path is AbstractFilePath)
            checkNotNull(fileSystem)
            val realPath = path.toRealPath()
            val pathNode = realPath.node

            return type.cast(
                MemoryFileAttributeView(
                    fileSystem = fileSystem,
                    name = "posix",
                    node = pathNode,
                )
            )
        } else if (BasicFileAttributeView::class.java == type) {
            require(path is AbstractFilePath)
            checkNotNull(fileSystem)
            val realPath = path.toRealPath()
            val pathNode = realPath.node

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
        require(path is AbstractFilePath)

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
        Require.todo()
    }

    override fun isSameFile(path1: Path, path2: Path): Boolean {
        require(path1 is HardFilePath)
        require(path2 is HardFilePath)

        val path1Node = path1.node
        val path2Node = path2.node
        return path1Node == path2Node
    }

    override fun newDirectoryStream(parent: Path, filter: DirectoryStream.Filter<in Path>): DirectoryStream<Path> {
        require(parent is HardFilePath)

        Check.isTrue { parent.node is DirectoryNode }

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
        require(path is AbstractFilePath)

        return path.isHidden()
    }
}