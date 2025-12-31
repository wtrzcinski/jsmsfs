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

import org.wtrzcinski.files.memory.node.DirectoryNode
import org.wtrzcinski.files.memory.util.Check
import org.wtrzcinski.files.memory.util.Require
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.isDirectory

sealed interface FilePath {
    companion object {
        fun Path.deleteRecursively() {
            if (this.isDirectory()) {
                Files.list(this).forEach { sub ->
                    sub.deleteRecursively()
                }
            }
            Files.delete(this)
        }

        fun getPath(root: HardFilePath, path: String, vararg more: String): Path {
            if (path == File.separator) {
                if (more.isEmpty()) {
                    return root
                }
            }

            val split = path.split(File.separator)
            val join = split + more
            return FilePath.resolve(root, join)
        }

        fun resolve(current: AbstractFilePath, other: Path): Path {
            require(other is AbstractFilePath)

            val otherName = other.name
            if (otherName.isBlank()) {
                return current
            }
            val thisName = current.name
            if (thisName.isBlank()) {
                return other
            }
            if (other.isAbsolute) {
                return other
            }
            return FilePath.resolve(current, otherName)
        }

        fun resolve(current: AbstractFilePath, opath: String): Path {
            val split = opath.split(File.separatorChar)
            return FilePath.resolve(current, split)
        }

        fun resolve(current: AbstractFilePath, split: List<String>): Path {
            val names = split.filter { it.isNotEmpty() }
            if (names.isEmpty()) {
                return current
            }

            val provider = current.fileSystem.provider()
            val actualFileSystem = provider.fileSystem
            requireNotNull(actualFileSystem)

            Check.isTrue { current.isAbsolute }

            var result: AbstractFilePath = current
            for (name in names) {
                if (result is HardFilePath) {
                    val directory = result.node
                    require(directory is DirectoryNode)

                    val existingNode = actualFileSystem.findChildByName(directory, name)
                    if (existingNode != null) {
                        result = HardFilePath(
                            fs = current.fs,
                            nodeRef = existingNode.offset,
                            parent = result,
                        )
                    } else {
                        result = SymbolicFilePath(
                            fs = current.fs,
                            name = name,
                            parent = result,
                        )
                    }
                } else if (result is SymbolicFilePath) {
                    result = SymbolicFilePath(
                        fs = current.fs,
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

    fun getParent(): FilePath?

    fun exists(): Boolean

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
            current = current.getParent()
        }
        return names.reversed()
    }
}