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

import org.wtrzcinski.memory.MemorySegmentFileSystem
import org.wtrzcinski.memory.mode.ModeMonitor
import org.wtrzcinski.memory.provider.attribute.MemoryFileAttributes.Companion.basic
import org.wtrzcinski.memory.provider.attribute.MemoryFileAttributes.Companion.owner
import org.wtrzcinski.memory.provider.attribute.MemoryFileAttributes.Companion.posix
import org.wtrzcinski.memory.provider.attribute.MemoryFileAttributes.Companion.user
import org.wtrzcinski.memory.util.RegexUtil
import java.io.File
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.util.regex.Pattern
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.io.path.fileStore

@OptIn(ExperimentalAtomicApi::class)
data class MemoryFileSystem(
    val env: Map<String, *>,
    val provider: MemoryFileSystemProvider,
) : FileSystem() {

    companion object {
        const val GLOB_SYNTAX: String = "glob"
        const val REGEX_SYNTAX: String = "regex"
    }

    private val monitor = ModeMonitor()

    val delegate: MemorySegmentFileSystem get() = checkNotNull(provider.fileSystem)

    val name: String get() = delegate.name

    val root = MemoryFilePathAdapter(delegate = delegate.root, fileSystem = this)

    override fun toString(): String {
        return "${javaClass.simpleName}(name=$name, env=$env, root=${delegate.root})"
    }

    override fun provider(): MemoryFileSystemProvider {
        return provider
    }

    override fun getPath(path: String, vararg more: String): Path {
        return MemoryFilePathAdapter(
            delegate = root.delegate.resolve(path, *more),
            fileSystem = this,
        )
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
        return delegate.isSafe()
    }

    override fun isOpen(): Boolean {
        return delegate.isAlive()
    }

    override fun supportedFileAttributeViews(): Set<String> {
        return setOf(basic, posix, user, owner)
    }

    /**
     * @see jdk.nio.zipfs.ZipFileSystem.getPathMatcher
     */
    override fun getPathMatcher(syntaxAndInput: String): PathMatcher {
        val pos: Int = syntaxAndInput.indexOf(':')
        require(pos > 0)
        val syntax = syntaxAndInput.substring(0, pos)
        val input = syntaxAndInput.substring(pos + 1)
        val expr: String?
        if (syntax.equals(GLOB_SYNTAX, ignoreCase = true)) {
            expr = RegexUtil.toRegexPattern(input)
        } else if (syntax.equals(REGEX_SYNTAX, ignoreCase = true)) {
            expr = input
        } else {
            throw UnsupportedOperationException("Syntax '$syntax' not recognized")
        }

        // return matcher
        val pattern = Pattern.compile(expr)
        return PathMatcher { path: Path ->
            val toUri = path.toUri()
            val uriPath = toUri.path
            pattern.matcher(uriPath).matches()
        }
    }

    override fun getUserPrincipalLookupService(): UserPrincipalLookupService {
        return MemoryUserPrincipalLookupService()
    }

    override fun newWatchService(): MemoryWatchService {
        return MemoryWatchService()
    }
}