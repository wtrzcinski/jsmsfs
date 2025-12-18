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

package org.wtrzcinski.files.memory.attribute

import org.wtrzcinski.files.memory.MemorySegmentFileSystem
import org.wtrzcinski.files.memory.node.NodeType
import org.wtrzcinski.files.memory.node.ValidNode
import java.nio.file.attribute.*

internal class MemoryFileAttributes(
    val fileSystem: MemorySegmentFileSystem,
    val name: String,
    val node: ValidNode,
    val attrs: AttributesBlock,
) : PosixFileAttributes {
    companion object {
        val basic = "basic"
        val posix = "posix"
        val user = "user"
        val owner = "owner"
        val acl = "acl"
    }

    override fun fileKey(): Any {
        return node.nodeRef
    }

    override fun isRegularFile(): Boolean {
        return node.fileType == NodeType.RegularFile
    }

    override fun isDirectory(): Boolean {
        return node.fileType == NodeType.Directory
    }

    override fun isSymbolicLink(): Boolean {
        return node.fileType == NodeType.SymbolicLink
    }

    override fun isOther(): Boolean {
        return !isRegularFile && !isDirectory && !isSymbolicLink
    }

    override fun lastAccessTime(): FileTime {
        return FileTime.from(attrs.lastAccessTime)
    }

    override fun lastModifiedTime(): FileTime {
        return FileTime.from(attrs.lastModifiedTime)
    }

    override fun creationTime(): FileTime {
        return FileTime.from(attrs.creationTime)
    }

    override fun permissions(): Set<PosixFilePermission> {
        return attrs.permissions
    }

    override fun owner(): UserPrincipal? {
        TODO("Not yet implemented")
    }

    override fun group(): GroupPrincipal? {
        TODO("Not yet implemented")
    }

    override fun size(): Long {
        TODO("Not yet implemented")
    }
}