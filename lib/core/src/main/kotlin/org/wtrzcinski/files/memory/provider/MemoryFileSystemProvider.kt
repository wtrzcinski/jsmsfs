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

import org.wtrzcinski.files.memory.MemorySegmentFileSystem
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.allocator.MemoryScopeType
import org.wtrzcinski.files.memory.buffer.MemoryOpenOptions
import org.wtrzcinski.files.memory.buffer.MemoryOpenOptions.Companion.CREATE_NEW
import org.wtrzcinski.files.memory.buffer.channel.FragmentedReadWriteBuffer
import org.wtrzcinski.files.memory.buffer.channel.MemoryFileChannel
import org.wtrzcinski.files.memory.exception.MemoryUnsupportedOperationException
import org.wtrzcinski.files.memory.node.*
import org.wtrzcinski.files.memory.node.NodeType.Directory
import org.wtrzcinski.files.memory.node.NodeType.SymbolicLink
import org.wtrzcinski.files.memory.node.attribute.MemoryFileAttributeView
import org.wtrzcinski.files.memory.node.attribute.MemoryFileAttributes
import org.wtrzcinski.files.memory.node.directory.MemorySecureDirectoryStream
import java.lang.AutoCloseable
import java.net.URI
import java.nio.ByteBuffer.allocate
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.FileChannel
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import java.nio.file.*
import java.nio.file.attribute.*
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.use

@OptIn(ExperimentalAtomicApi::class)
@Suppress("UNCHECKED_CAST")
internal class MemoryFileSystemProvider(
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
                val context = MemorySegmentFileSystem(
                    scope = scope,
                    byteSize = capacity,
                    blockSize = blockSize,
                    name = rawQuery,
                    env = env
                )
                val fileSystem = MemoryFileSystem(
                    env = env,
                    name = rawQuery,
                    provider = MemoryFileSystemProvider(fileSystem = context)
                )
                filesystems[rawQuery] = fileSystem
                return fileSystem
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

    override fun newByteChannel(path: Path, options: Set<OpenOption>, vararg attrs: FileAttribute<*>): FragmentedReadWriteBuffer {
        requireNotNull(fileSystem)
        require(path is MemoryPath)

        val mode = MemoryOpenOptions(options as Set<StandardOpenOption>)
        val parent = path.parent?.node
        val child = path.node
        require(parent is DirectoryNode)
        return fileSystem.getOrCreateDataChannel(parent = parent, childName = child.name, mode = mode)
    }

    override fun newFileChannel(path: Path, options: Set<OpenOption>, vararg attrs: FileAttribute<*>): FileChannel {
        return MemoryFileChannel(delegate = newByteChannel(path, options, *attrs))
    }

    override fun newAsynchronousFileChannel(
        path: Path?,
        options: Set<OpenOption?>?,
        executor: ExecutorService?,
        vararg attrs: FileAttribute<*>?
    ): AsynchronousFileChannel? {
        TODO("Not yet implemented")
    }

    override fun createDirectory(childPath: Path, vararg attrs: FileAttribute<*>) {
        requireNotNull(fileSystem)
        require(childPath is MemoryPath)

        val parentPath = childPath.parent
        if (parentPath != null) {
            createDirectory(parentPath, *attrs)
        }

        val parentNode = parentPath?.node
        require(parentNode is DirectoryNode?)

        val childNode = childPath.node
        if (childNode is InvalidNode) {
            require(parentNode != null)

            fileSystem.getOrCreateFile(parent = parentNode, childType = Directory, childName = childNode.name, targetNode = null, mode = CREATE_NEW)
            require(childPath.node is DirectoryNode)
        }
    }

    override fun createLink(link: Path?, existing: Path?) {
        super.createLink(link, existing)
    }

    override fun createSymbolicLink(link: Path, target: Path, vararg attrs: FileAttribute<*>) {
        requireNotNull(fileSystem)
        require(link is MemoryPath)
        require(target is MemoryPath)

        val parentPath = link.parent
        if (parentPath != null) {
            createDirectory(parentPath, *attrs)
        }

        val parentNode = parentPath?.node
        require(parentNode is DirectoryNode?)

        val targetNode = target.node
        require(targetNode is RegularFileNode || targetNode is DirectoryNode)

        val childNode = link.node
        if (childNode is InvalidNode) {
            require(parentNode != null)

            fileSystem.getOrCreateFile(parent = parentNode, childType = SymbolicLink, childName = childNode.name, targetNode = targetNode, mode = CREATE_NEW)
            require(link.node is SymbolicLinkNode)
        }
    }

    override fun move(source: Path, target: Path, vararg options: CopyOption) {
        require(source is MemoryPath)
        require(target is MemoryPath)

        copy(source, target, *options)

        delete(source)
    }

    override fun copy(source: Path?, target: Path?, vararg options: CopyOption) {
        require(source is MemoryPath)
        require(target is MemoryPath)

        val sourceByteBuffer = Files.newByteChannel(source, StandardOpenOption.READ)
        sourceByteBuffer.use<ReadableByteChannel, Unit> { input ->
            val targetByteBuffer = Files.newByteChannel(target, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
            targetByteBuffer.use<WritableByteChannel, Unit> { output ->
                val buffer = allocate(1024 * 4)
                while (true) {
                    buffer.clear()
                    val length = input.read(buffer)
                    if (length < 0) {
                        return
                    }
                    buffer.flip()
                    output.write(buffer)
                }
            }
        }
    }

    override fun delete(path: Path) {
        require(path is MemoryPath)
        checkNotNull(fileSystem)

        val node = path.node
        if (node is ValidNode) {
            val parentNode = path.parent?.node
            this.fileSystem.delete(parentNode, node)
        }
    }

    override fun <A : BasicFileAttributes> readAttributes(path: Path, type: Class<A>, vararg options: LinkOption): A {
        require(path is MemoryPath)

        val pathNode = path.node
        if (pathNode is InvalidNode) {
            throw NoSuchFileException(toString())
        }
        require(pathNode is ValidNode)
        if (PosixFileAttributes::class.java == type) {
            requireNotNull(fileSystem)
            val findAttrs = fileSystem.findAttrs(pathNode)
            return type.cast(MemoryFileAttributes(
                fileSystem = fileSystem,
                name = "posix",
                node = pathNode,
                attrs = findAttrs,
            ))
        } else if (BasicFileAttributes::class.java == type) {
            requireNotNull(fileSystem)
            val findAttrs = fileSystem.findAttrs(pathNode)
            return type.cast(MemoryFileAttributes(
                fileSystem = fileSystem,
                name = "basic",
                node = pathNode,
                attrs = findAttrs,
            ))
        }
        throw MemoryUnsupportedOperationException()
    }

    override fun <V : FileAttributeView?> getFileAttributeView(path: Path, type: Class<V>, vararg options: LinkOption): V {
        require(path is MemoryPath)

        val pathNode = path.node
        if (pathNode is InvalidNode) {
            throw NoSuchFileException(toString())
        }
        require(pathNode is ValidNode)
        if (PosixFileAttributeView::class.java == type) {
            requireNotNull(fileSystem)
            return type.cast(MemoryFileAttributeView(
                fileSystem = fileSystem,
                name = "posix",
                node = pathNode,
            ))
        } else if (BasicFileAttributeView::class.java == type) {
            requireNotNull(fileSystem)
            return type.cast(MemoryFileAttributeView(
                fileSystem = fileSystem,
                name = "basic",
                node = pathNode,
            ))
        }
        throw MemoryUnsupportedOperationException()
    }

    override fun checkAccess(path: Path, vararg modes: AccessMode) {
        require(path is MemoryPath)

        val pathNode = path.node
        if (pathNode is InvalidNode) {
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
        require(path1 is MemoryPath)
        require(path2 is MemoryPath)

        val path1Node = path1.node
        val path2Node = path2.node
        return path1Node == path2Node
    }

    override fun newDirectoryStream(parent: Path, filter: DirectoryStream.Filter<in Path>): DirectoryStream<Path> {
        require(parent is MemoryPath)

        if (parent.isAbsolute) {
            return MemorySecureDirectoryStream(parent = parent, filter = filter)
        }
        TODO("Not yet implemented")
    }

    override fun getPath(uri: URI): Path {
        val fileSystem = getFileSystem(uri)
        val path = uri.path
        return fileSystem.getPath(path)
    }

    override fun getFileStore(path: Path?): FileStore {
        checkNotNull(fileSystem)

        return MemoryFileStore(fileSystem.bitmapRegistry, fileSystem.memory)
    }

    override fun isHidden(path: Path): Boolean {
        require(path is MemoryPath)

        val toStringList = path.toStringList()
        return toStringList.any { it.startsWith(".") }
    }
}