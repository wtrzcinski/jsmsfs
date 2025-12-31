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

package org.wtrzcinski.files.memory.path

import org.wtrzcinski.files.memory.provider.MemoryFileSystem
import org.wtrzcinski.files.memory.util.Require
import java.io.File
import java.net.URI
import java.nio.file.*

sealed class AbstractFilePath(
    val fs: MemoryFileSystem,
) : Path, FilePath {

    abstract override fun toRealPath(vararg options: LinkOption): HardFilePath

    abstract override fun getParent(): AbstractFilePath?

    override fun toString(): String {
        return toUri().toString()
    }

    override fun compareTo(other: Path): Int {
        return toUri().compareTo(other.toUri())
    }

    override fun toUri(): URI {
        val provider = fileSystem.provider()

        val scheme = provider.scheme
        val joinPath = getNames().joinToString(File.separator)
        val name = fileSystem.name
        return if (name.isNotBlank()) {
            URI.create("$scheme:///$joinPath?$name")
        } else {
            URI.create("$scheme:///$joinPath")
        }
    }

    override fun getFileSystem(): MemoryFileSystem {
        return fs
    }

    override fun resolve(other: Path): Path {
        return FilePath.resolve(current = this, other = other)
    }

    override fun resolve(opath: String): Path {
        return FilePath.resolve(current = this, opath = opath)
    }

    fun getAncestors(): List<AbstractFilePath> {
        val result = mutableListOf<AbstractFilePath>()
        var current: AbstractFilePath? = this
        while (current != null) {
            result.add(current)
            current = current.getParent()
        }
        return result.reversed()
    }

    override fun getRoot(): Path? {
        val localParent = this.parent
        if (localParent != null) {
            return localParent.getRoot()
        }
        return this
    }

    override fun getFileName(): Path? {
        val names = getNames()
        val count = names.count()
        if (count == 0) {
            return null
        } else if (count == 1) {
            return this
        } else {
            return SymbolicFilePath(
                fs = fs,
                parent = parent,
                name = names.last(),
                absolute = false,
            )
        }
    }

    override fun getNameCount(): Int {
        return getNames().count()
    }

    override fun getName(index: Int): Path {
        val ancestors = getAncestors()
        return ancestors[0]
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