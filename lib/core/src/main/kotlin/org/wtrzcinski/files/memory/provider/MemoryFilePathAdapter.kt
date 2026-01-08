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

package org.wtrzcinski.files.memory.provider

import org.wtrzcinski.files.memory.MemorySegmentFileSystem
import org.wtrzcinski.files.memory.path.FilePath
import org.wtrzcinski.files.memory.path.SymbolicFilePath
import org.wtrzcinski.files.memory.util.Require
import java.io.File
import java.net.URI
import java.nio.file.*
import kotlin.io.path.isDirectory

class MemoryFilePathAdapter(
    val delegate: FilePath,
    private val fileSystem: MemoryFileSystem,
) : Path {
    companion object {
        fun Path.deleteRecursively() {
            if (this.isDirectory()) {
                Files.list(this).forEach { sub ->
                    sub.deleteRecursively()
                }
            }
            Files.delete(this)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is MemoryFilePathAdapter) return false

        if (delegate != other.delegate) return false

        return true
    }

    override fun hashCode(): Int {
        return delegate.hashCode()
    }

    override fun toRealPath(vararg options: LinkOption): MemoryFilePathAdapter {
        return MemoryFilePathAdapter(delegate.toRealPath(), fileSystem)
    }

    override fun getParent(): MemoryFilePathAdapter? {
        val delegate1 = delegate.parent ?: return null
        return MemoryFilePathAdapter(delegate1, fileSystem)
    }

    fun actualFileSystem(): MemorySegmentFileSystem {
        val actualFileSystem = fileSystem.provider().fileSystem
        return checkNotNull(actualFileSystem)
    }

    fun exists(): Boolean {
        return delegate.exists()
    }

    override fun toString(): String {
        return toUri().toString()
    }

    override fun compareTo(other: Path): Int {
        return toUri().compareTo(other.toUri())
    }

    override fun toUri(): URI {
        val provider = fileSystem.provider()

        val scheme = provider.scheme
        val joinPath = delegate.getNames().joinToString(File.separator)
        val name = fileSystem.name
        return if (name.isNotBlank()) {
            URI.create("$scheme:///$joinPath?$name")
        } else {
            URI.create("$scheme:///$joinPath")
        }
    }

    override fun getFileSystem(): MemoryFileSystem {
        return fileSystem
    }

    override fun resolve(other: Path): MemoryFilePathAdapter {
        require(other is MemoryFilePathAdapter)

        return MemoryFilePathAdapter(
            delegate = FilePath.resolve(current = delegate, other = other.delegate),
            fileSystem = fileSystem,
        )
    }

    override fun resolve(opath: String): MemoryFilePathAdapter {
        return MemoryFilePathAdapter(
            delegate = FilePath.resolve(current = this.delegate, path = opath),
            fileSystem = fileSystem,
        )
    }

    override fun getRoot(): MemoryFilePathAdapter? {
        val localParent = this.parent
        if (localParent != null) {
            return localParent.getRoot()
        }
        return this
    }

    override fun getFileName(): MemoryFilePathAdapter? {
        val names = delegate.getNames()
        val count = names.count()
        if (count == 0) {
            return null
        } else if (count == 1) {
            return MemoryFilePathAdapter(
                delegate = SymbolicFilePath(
                    fileSystem = actualFileSystem(),
                    parent = parent?.delegate,
                    name = names.last(),
                    absolute = true,
                ),
                fileSystem = fileSystem
            )
        } else {
            return MemoryFilePathAdapter(
                delegate = SymbolicFilePath(
                    fileSystem = actualFileSystem(),
                    parent = parent?.delegate,
                    name = names.last(),
                    absolute = false,
                ),
                fileSystem = fileSystem,
            )
        }
    }

    override fun getNameCount(): Int {
        return delegate.getNames().count()
    }

    override fun getName(index: Int): MemoryFilePathAdapter {
        val ancestors = delegate.getAncestors()
        val ancestor = ancestors[0]
        return MemoryFilePathAdapter(
            delegate = ancestor,
            fileSystem = fileSystem,
        )
    }

    override fun isAbsolute(): Boolean {
        return delegate.isAbsolute()
    }

    override fun toAbsolutePath(): MemoryFilePathAdapter {
        return MemoryFilePathAdapter(
            delegate = delegate.toAbsolutePath(),
            fileSystem = fileSystem,
        )
    }

    override fun subpath(beginIndex: Int, endIndex: Int): Path {
        Require.todo()
    }

    override fun startsWith(other: Path): Boolean {
        Require.todo()
    }

    override fun endsWith(other: Path): Boolean {
        Require.todo()
    }

    override fun normalize(): Path {
        Require.todo()
    }

    override fun relativize(other: Path): Path {
        Require.todo()
    }

    override fun register(watcher: WatchService, events: Array<out WatchEvent.Kind<*>>, vararg modifiers: WatchEvent.Modifier?): WatchKey {
        Require.todo()
    }
}