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

package org.wtrzcinski.memory.path

import org.wtrzcinski.memory.MemorySegmentFileSystem
import org.wtrzcinski.memory.mapper.NodeMapper
import org.wtrzcinski.memory.mapper.NodeType
import org.wtrzcinski.memory.util.Check
import java.io.File
import java.net.URI
import java.nio.file.NoSuchFileException

sealed interface FilePath {

    val name: String

    val type: NodeType

    val parent: FilePath?

    val fileSystem: MemorySegmentFileSystem

    fun resolve(other: FilePath): FilePath {
        val otherName = other.name
        if (otherName.isBlank()) {
            return this
        }
        val thisName = this.name
        if (thisName.isBlank()) {
            return other
        }
        if (other.isAbsolute()) {
            return other
        }
        return resolve(path = otherName)
    }

    fun resolve(path: String, vararg more: String): FilePath {
        if (path == File.separator) {
            if (more.isEmpty()) {
                return this
            }
        }

        val split = path.split(File.separator)
        return resolve(split + more)
    }

    fun resolve(path: String): FilePath {
        return resolve(path.split(File.separatorChar))
    }

    fun resolve(split: List<String>): FilePath {
        val names = split.filter { it.isNotEmpty() }
        if (names.isEmpty()) {
            return this
        }

        Check.isTrue { this.isAbsolute() }

        var result: FilePath = this
        for (name in names) {
            if (result is RealFilePath) {
                val directory = result.node
                require(result.type == NodeType.Directory)

                val existingNode = directory.findChildByName(name)
                if (existingNode != null) {
                    result = HardFilePath(
                        fileSystem = this.fileSystem,
                        ref = existingNode.ref(),
                        parent = result,
                        node = existingNode,
                    )
                } else {
                    result = SymbolicFilePath(
                        fileSystem = this.fileSystem,
                        name = name,
                        parent = result,
                    )
                }
            } else if (result is SymbolicFilePath) {
                result = SymbolicFilePath(
                    fileSystem = this.fileSystem,
                    name = name,
                    parent = result,
                )
            } else {
                TODO("Not yet implemented")
            }
        }
        return result
    }

    @Throws(NoSuchFileException::class)
    fun toRealPath(): RealFilePath

    fun toUri(scheme: String = "memory"): URI {
        val joinPath = getNames().joinToString(File.separator)
        return if (fileSystem.name.isNotBlank()) {
            URI.create("$scheme:///$joinPath?$fileSystem.name")
        } else {
            URI.create("$scheme:///$joinPath")
        }
    }

    fun isAbsolute(): Boolean

    fun toAbsolutePath(): FilePath

    fun isDirectory(): Boolean

    fun isHidden(): Boolean {
        return getNames().any { it.startsWith(".") }
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