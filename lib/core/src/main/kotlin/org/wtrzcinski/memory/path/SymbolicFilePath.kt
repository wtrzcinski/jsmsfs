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
import org.wtrzcinski.memory.mapper.NodeType
import java.nio.file.NoSuchFileException

class SymbolicFilePath(
    override val fileSystem: MemorySegmentFileSystem,
    override val name: String,
    override val parent: FilePath? = null,
) : FilePath {

    @Throws(NoSuchFileException::class)
    override fun toRealPath(): HardFilePath {
        val node = findNode() ?: throw NoSuchFileException(toString())
        val parent1 = checkNotNull(parent).toRealPath()
        return HardFilePath(
            fileSystem = fileSystem,
            ref = node.ref(),
            parent = parent1,
            node = node,
        )
    }

    override val type: NodeType = NodeType.Unknown

    override fun isDirectory(): Boolean {
        return false
    }

    override fun isAbsolute(): Boolean {
        return parent != null
    }

    override fun toAbsolutePath(): FilePath {
        if (this.isAbsolute()) {
            return this
        }
        require(parent == null)
        return fileSystem.root.resolve(name)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SymbolicFilePath) return false

        if (name != other.name) return false
        if (parent != other.parent) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + (parent?.hashCode() ?: 0)
        return result
    }

}