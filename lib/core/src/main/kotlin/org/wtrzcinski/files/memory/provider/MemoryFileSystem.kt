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
import org.wtrzcinski.files.memory.mode.AbstractCloseable
import org.wtrzcinski.files.memory.path.FilePath
import org.wtrzcinski.files.memory.path.HardFilePath
import org.wtrzcinski.files.memory.provider.MemoryFileAttributes.Companion.basic
import org.wtrzcinski.files.memory.provider.MemoryFileAttributes.Companion.owner
import org.wtrzcinski.files.memory.provider.MemoryFileAttributes.Companion.posix
import org.wtrzcinski.files.memory.provider.MemoryFileAttributes.Companion.user
import java.io.File
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.io.path.fileStore

@OptIn(ExperimentalAtomicApi::class)
data class MemoryFileSystem(
    val name: String,
    val env: Map<String, *>,
    val provider: MemoryFileSystemProvider,
) : FileSystem() {

    private val monitor = AbstractCloseable()

    val delegate: MemorySegmentFileSystem
        get() {
            val fileSystem = provider.fileSystem
            requireNotNull(fileSystem)
            return fileSystem
        }

    internal val root: HardFilePath = HardFilePath(
        fs = this,
        parent = null,
        nodeRef = delegate.rootRef,
    )

    override fun toString(): String {
        return "${javaClass.simpleName}(name=$name, env=$env, root=$root)"
    }

    override fun provider(): MemoryFileSystemProvider {
        return provider
    }

    override fun getPath(path: String, vararg more: String): Path {
        return FilePath.getPath(root, path, *more)
    }

    override fun getRootDirectories(): Iterable<Path> {
        return listOf(root)
    }

    override fun getSeparator(): String {
        return File.separator
    }

    override fun close() {
        if (monitor.tryClose()) {
            provider.close()
        }
    }

    override fun getFileStores(): Iterable<MemoryFileStore> {
        val fileStore = root.fileStore()
        require(fileStore is MemoryFileStore)
        return listOf(fileStore)
    }

    override fun isReadOnly(): Boolean {
        return delegate.isReadOnly()
    }

    override fun isOpen(): Boolean {
        return delegate.isAlive()
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