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

import org.wtrzcinski.files.memory.directory.DirectoryNode
import org.wtrzcinski.files.memory.node.InvalidNode
import org.wtrzcinski.files.memory.node.Node
import java.io.File
import java.net.URI
import java.nio.file.*

internal data class SimpleMemoryPath(
    val fs: SimpleMemoryFileSystem,
    val parent: SimpleMemoryPath?,
    private val nodeSupplier: (SimpleMemoryPath?) -> Node,
    private val absolute: Boolean = true,
) : Path {

    val node: Node get() = nodeSupplier(parent)

    override fun getFileSystem(): SimpleMemoryFileSystem {
        return fs
    }

    override fun resolve(other: Path): Path {
        require(other is SimpleMemoryPath)

        val otherNode = other.node
        if (otherNode.name.isBlank()) {
            return this
        }
        val thisNode = this.node
        if (thisNode.name.isBlank()) {
            return other
        }
        if (other.isAbsolute) {
            return other
        }
        return resolve(otherNode.name)
    }

    override fun resolve(opath: String): SimpleMemoryPath {
        return resolve(opath.split(File.separatorChar))
    }

    fun resolve(split: List<String>): SimpleMemoryPath {
        val names = split.filter { it.isNotEmpty() }
        if (names.isEmpty()) {
            return this
        }

        val actualFileSystem = fileSystem.provider().fileSystem
        requireNotNull(actualFileSystem)

        var result: SimpleMemoryPath = this
        for (name in names) {
            result = result.resolve { parent ->
                val directory = parent?.node
                require(directory != null)

                var existing: Node? = null
                if (directory is DirectoryNode) {
                    existing = actualFileSystem.findChildByName(directory, name)
                }
                if (existing == null) {
                    existing = InvalidNode(name = name)
                }
                existing
            }
        }
        return result
    }

    private fun resolve(nodeSupplier: (SimpleMemoryPath?) -> Node): SimpleMemoryPath {
        return SimpleMemoryPath(fs = fs, parent = this, nodeSupplier = nodeSupplier, absolute = true)
    }

    override fun isAbsolute(): Boolean {
        return absolute
    }

    override fun getRoot(): Path? {
        if (this.parent != null) {
            return this.parent.getRoot()
        }
        return null
    }

    override fun getParent(): Path? {
        return parent
    }

    override fun toUri(): URI {
        val provider = fileSystem.provider()

        val scheme = provider.scheme
        val joinToString = toStringList().joinToString(File.separator)
        val name = fileSystem.name
        if (name.isNotBlank()) {
            return URI.create("$scheme:/$joinToString?$name")
        } else {
            return URI.create("$scheme:/$joinToString")
        }
    }

    fun toStringList(): Sequence<String> {
        val names = toNodeList()
        return names
            .map { it.name }
            .filter { it != File.separator }
    }

    fun toNodeList(): Sequence<Node> {
        val nodes = mutableListOf<Node>()
        var current: SimpleMemoryPath? = this
        while (current != null) {
            val node1 = current.node
            nodes.add(node1)
            current = current.parent
        }
        val result = nodes.reversed()
        return result.asSequence()
    }

    override fun toString(): String {
        return toUri().toString()
    }

    override fun toAbsolutePath(): Path {
        if (this.isAbsolute) {
            return this
        }
        TODO("Not yet implemented")
    }

    override fun getNameCount(): Int {
        val toStringList = toStringList()
        return toStringList.count()
    }

    override fun getFileName(): Path? {
        TODO("Not yet implemented")
    }

    override fun getName(index: Int): Path {
        TODO("Not yet implemented")
    }

    override fun subpath(beginIndex: Int, endIndex: Int): Path {
        TODO("Not yet implemented")
    }

    override fun startsWith(other: Path): Boolean {
        TODO("Not yet implemented")
    }

    override fun endsWith(other: Path): Boolean {
        TODO("Not yet implemented")
    }

    override fun normalize(): Path {
        TODO("Not yet implemented")
    }

    override fun relativize(other: Path): Path {
        TODO("Not yet implemented")
    }

    override fun toRealPath(vararg options: LinkOption): Path {
        TODO("Not yet implemented")
    }

    override fun register(watcher: WatchService, events: Array<out WatchEvent.Kind<*>>, vararg modifiers: WatchEvent.Modifier?): WatchKey {
        TODO("Not yet implemented")
    }

    override fun compareTo(other: Path): Int {
        TODO("Not yet implemented")
    }
}