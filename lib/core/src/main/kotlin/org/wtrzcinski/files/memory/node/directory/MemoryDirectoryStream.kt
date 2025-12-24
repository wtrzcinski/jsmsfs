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

package org.wtrzcinski.files.memory.node.directory

import org.wtrzcinski.files.memory.node.DirectoryNode
import org.wtrzcinski.files.memory.node.MemoryPath
import java.nio.file.DirectoryStream
import java.nio.file.Path

internal class MemoryDirectoryStream(
    val path: MemoryPath,
    val filter: DirectoryStream.Filter<in Path>,
) : DirectoryStream<Path> {
    override fun iterator(): MutableIterator<MemoryPath> {
        val node = path.node
        require(node is DirectoryNode)

        val actualFileSystem = path.fileSystem.provider().fileSystem
        requireNotNull(actualFileSystem)

        val children = actualFileSystem.findChildren(node)
        val map = children.map { path.resolve(it.name) }
        val filter = map.filter { filter.accept(it) }
        val iterator = filter.iterator()

        return object : MutableIterator<MemoryPath> {
            private lateinit var current: MemoryPath

            override fun next(): MemoryPath {
                val next = iterator.next()
                current = next
                return next
            }

            override fun hasNext(): Boolean {
                return iterator.hasNext()
            }

            override fun remove() {
                val provider = current.fileSystem.provider()
                provider.delete(current)
            }
        }
    }

    override fun close() {
    }
}