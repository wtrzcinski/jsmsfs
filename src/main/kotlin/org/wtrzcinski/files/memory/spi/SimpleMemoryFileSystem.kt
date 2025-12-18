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

package org.wtrzcinski.files.memory.spi

import org.wtrzcinski.files.memory.MemorySegmentFileSystem
import org.wtrzcinski.files.memory.attribute.MemoryFileAttributes.Companion.basic
import org.wtrzcinski.files.memory.attribute.MemoryFileAttributes.Companion.owner
import org.wtrzcinski.files.memory.attribute.MemoryFileAttributes.Companion.posix
import org.wtrzcinski.files.memory.attribute.MemoryFileAttributes.Companion.user
import java.io.File
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.io.path.fileStore

@OptIn(ExperimentalAtomicApi::class)
internal data class SimpleMemoryFileSystem(
    val name: String,
    val env: Map<String, *>,
    val provider: SimpleMemoryFileSystemProvider,
) : FileSystem() {

    private val closed = AtomicBoolean(false)

    val delegate: MemorySegmentFileSystem get() {
        val fileSystem = provider.fileSystem
        requireNotNull(fileSystem)
        return fileSystem
    }

    val root: SimpleMemoryPath = SimpleMemoryPath(
        fs = this,
        parent = null,
        nodeSupplier = { delegate.root() },
    )

    override fun toString(): String {
        return "${javaClass.simpleName}(name=$name, env=$env, root=$root)"
    }

    override fun provider(): SimpleMemoryFileSystemProvider {
        return provider
    }

    override fun getPath(path: String, vararg more: String): Path {
        if (path == File.separator) {
            if (more.isEmpty()) {
                return root
            }
        }

        val split = path.split(File.separator)
        val join = split + more
        return root.resolve(join)
    }

    override fun getRootDirectories(): Iterable<Path> {
        return listOf(root)
    }

    override fun getSeparator(): String {
        return File.separator
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            provider.close()
        }
    }

    override fun getFileStores(): Iterable<SimpleMemoryFileStore> {
        val fileStore = root.fileStore()
        require(fileStore is SimpleMemoryFileStore)
        return listOf(fileStore)
    }

    override fun isReadOnly(): Boolean {
        return delegate.memory.isReadOnly()
    }

    override fun isOpen(): Boolean {
        return delegate.memory.scope().isAlive()
    }

    override fun supportedFileAttributeViews(): Set<String> {
        return setOf(basic, posix, user, owner)
    }

    //    todo test Files#setOwner(Path path, UserPrincipal owner)
    override fun getUserPrincipalLookupService(): UserPrincipalLookupService {
        TODO("Not yet implemented")
    }

//    todo test Files#newDirectoryStream(Path dir, String glob)
    override fun getPathMatcher(syntaxAndPattern: String?): PathMatcher? {
        TODO("Not yet implemented")
    }

    override fun newWatchService(): WatchService? {
        TODO("Not yet implemented")
    }
}