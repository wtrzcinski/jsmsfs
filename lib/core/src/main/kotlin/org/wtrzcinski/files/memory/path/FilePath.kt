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

package org.wtrzcinski.files.memory.path

import org.wtrzcinski.files.memory.MemorySegmentFileSystem
import org.wtrzcinski.files.memory.mapper.NodeMapper
import org.wtrzcinski.files.memory.mapper.NodeType
import org.wtrzcinski.files.memory.util.Check
import org.wtrzcinski.files.memory.util.Require
import java.io.File

sealed interface FilePath {
    companion object {

        fun resolve(root: FilePath, path: String, vararg more: String): FilePath {
            if (path == File.separator) {
                if (more.isEmpty()) {
                    return root
                }
            }

            val split = path.split(File.separator)
            val join = split + more
            return resolve(root, join)
        }

        fun resolve(current: FilePath, other: FilePath): FilePath {
            val otherName = other.name
            if (otherName.isBlank()) {
                return current
            }
            val thisName = current.name
            if (thisName.isBlank()) {
                return other
            }
            if (other.isAbsolute()) {
                return other
            }
            return resolve(current, otherName)
        }

        fun resolve(current: FilePath, path: String): FilePath {
            val split = path.split(File.separatorChar)
            return resolve(current, split)
        }

        fun resolve(current: FilePath, split: List<String>): FilePath {
            val names = split.filter { it.isNotEmpty() }
            if (names.isEmpty()) {
                return current
            }

            Check.isTrue { current.isAbsolute() }

            var result: FilePath = current
            for (name in names) {
                if (result is HardFilePath) {
                    val directory = result.node
                    require(directory.readType() == NodeType.Directory)

                    val existingNode = directory.findChildByName(name)
                    if (existingNode != null) {
                        result = HardFilePath(
                            fileSystem = current.fileSystem,
                            ref = existingNode.ref(),
                            parent = result,
                            node = existingNode
                        )
                    } else {
                        result = SymbolicFilePath(
                            fileSystem = current.fileSystem,
                            name = name,
                            parent = result,
                        )
                    }
                } else if (result is SymbolicFilePath) {
                    result = SymbolicFilePath(
                        fileSystem = current.fileSystem,
                        name = name,
                        parent = result,
                    )
                } else {
                    Require.todo()
                }
            }
            return result
        }
    }

    val name: String

    val type: NodeType

    val parent: FilePath?

    val fileSystem: MemorySegmentFileSystem

    fun toRealPath(): HardFilePath

    fun isAbsolute(): Boolean

    fun toAbsolutePath(): FilePath

    fun isDirectory(): Boolean

    fun isHidden(): Boolean {
        val strings = getNames()
        return strings.any { it.startsWith(".") }
    }

    fun getNames(): List<String> {
        val names = mutableListOf<String>()
        var current: FilePath? = this
        while (current != null) {
            val name = current.name
            if (name != File.separator && name.isNotEmpty()) {
                names.add(name)
            }
            current = current.parent
        }
        return names.reversed()
    }

    fun getAncestors(): List<FilePath> {
        val result = mutableListOf<FilePath>()
        var current: FilePath? = this
        while (current != null) {
            result.add(current)
            current = current.parent
        }
        return result.reversed()
    }

    fun exists(): Boolean {
        return findNode() != null
    }

    fun findNode(): NodeMapper? {
        val realParent = parent?.toRealPath()
        checkNotNull(realParent)
        if (!realParent.exists()) {
            return null
        }
        val parentNode = realParent.node
        check(realParent.type == NodeType.Directory)

        return parentNode.findChildByName(name = name)
    }
}