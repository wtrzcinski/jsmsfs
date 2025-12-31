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

import java.net.URI
import java.nio.file.*

class CacheDelegateFilePath(
    val delegate: AbstractFilePath
) : Path, FilePath {

    override val name: String get() = delegate.name

    override fun exists(): Boolean {
        return delegate.exists()
    }

    override fun isDirectory(): Boolean {
        return delegate.isDirectory()
    }

    override fun getFileSystem(): FileSystem {
        return delegate.getFileSystem()
    }

    override fun isAbsolute(): Boolean {
        return delegate.isAbsolute
    }

    override fun getRoot(): Path? {
        return delegate.getRoot()
    }

    override fun getFileName(): Path? {
        return delegate.getFileName()
    }

    override fun getParent(): AbstractFilePath? {
        return delegate.parent
    }

    override fun getNameCount(): Int {
        return delegate.nameCount
    }

    override fun getName(index: Int): Path {
        return delegate.getName(index)
    }

    override fun subpath(beginIndex: Int, endIndex: Int): Path {
        return delegate.subpath(beginIndex, endIndex)
    }

    override fun startsWith(other: Path): Boolean {
        return delegate.startsWith(other)
    }

    override fun endsWith(other: Path): Boolean {
        return delegate.endsWith(other)
    }

    override fun normalize(): Path {
        return delegate.normalize()
    }

    override fun resolve(other: Path): Path {
        return delegate.resolve(other)
    }

    override fun relativize(other: Path): Path {
        return delegate.relativize(other)
    }

    override fun toUri(): URI {
        return delegate.toUri()
    }

    override fun toAbsolutePath(): Path {
        return delegate.toAbsolutePath()
    }

    override fun toRealPath(vararg options: LinkOption): Path {
        return delegate.toRealPath(*options)
    }

    override fun register(watcher: WatchService, events: Array<out WatchEvent.Kind<*>>, vararg modifiers: WatchEvent.Modifier?): WatchKey {
        return delegate.register(watcher, events, *modifiers)
    }

    override fun compareTo(other: Path): Int {
        return delegate.compareTo(other)
    }

    override fun toString(): String {
        return delegate.toString()
    }
}