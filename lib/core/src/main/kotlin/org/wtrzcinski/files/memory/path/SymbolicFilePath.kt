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
import org.wtrzcinski.files.memory.node.ValidNode
import org.wtrzcinski.files.memory.provider.MemoryFileSystem
import org.wtrzcinski.files.memory.util.Require
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.Path

class SymbolicFilePath(
    fs: MemoryFileSystem,
    override val name: String,
    private val parent: AbstractFilePath?,
    private val absolute: Boolean = true,
) : AbstractFilePath(fs) {

    override fun getParent(): AbstractFilePath? {
        return parent
    }

    override fun exists(): Boolean {
        return findNode() != null
    }

    override fun isDirectory(): Boolean {
        return false
    }

    override fun isAbsolute(): Boolean {
        return absolute
    }

    override fun toAbsolutePath(): Path {
        if (this.isAbsolute) {
            return this
        }
        Require.todo()
    }

    override fun toRealPath(vararg options: LinkOption): HardFilePath {
        val node = findNodeOrThrow()
        val realParent = this.parent?.toRealPath()
        return HardFilePath(
            fs = fs,
            nodeRef = node.offset,
            parent = realParent,
        )
    }

    /**
     * @throws NoSuchFileException
     */
    fun findNodeOrThrow(): ValidNode {
        return findNode() ?: throw NoSuchFileException(toString())
    }

    fun findNode(): ValidNode? {
        val actualFileSystem = fileSystem.provider().fileSystem
        checkNotNull(actualFileSystem)

        val realParent = this.parent?.toRealPath()
        val parentNode = realParent?.node
        require(parentNode is DirectoryNode)

        return actualFileSystem.findChildByName(parent = parentNode, name = name)
    }
}