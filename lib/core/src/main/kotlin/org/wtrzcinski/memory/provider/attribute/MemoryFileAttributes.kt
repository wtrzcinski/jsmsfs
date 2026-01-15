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

package org.wtrzcinski.memory.provider.attribute

import org.wtrzcinski.memory.MemorySegmentFileSystem
import org.wtrzcinski.memory.mapper.AttrsMapper
import org.wtrzcinski.memory.mapper.NodeMapper
import org.wtrzcinski.memory.mapper.NodeType
import java.nio.file.attribute.*
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
internal class MemoryFileAttributes(
    val fileSystem: MemorySegmentFileSystem,
    val name: String,
    val node: NodeMapper,
    val attrs: AttrsMapper,
) : PosixFileAttributes {
    companion object {
        const val basic = "basic"
        const val posix = "posix"
        const val user = "user"
        const val owner = "owner"
        const val acl = "acl"
    }

    override fun fileKey(): Any {
        return node.ref()
    }

    override fun isRegularFile(): Boolean {
        return node.readType() == NodeType.Regular
    }

    override fun isDirectory(): Boolean {
        return node.readType() == NodeType.Directory
    }

    override fun isSymbolicLink(): Boolean {
        return node.readType() == NodeType.SymbolicLink
    }

    override fun isOther(): Boolean {
        return !isRegularFile && !isDirectory && !isSymbolicLink
    }

    override fun lastAccessTime(): FileTime {
        return FileTime.from(attrs.readLastAccessTime())
    }

    override fun lastModifiedTime(): FileTime {
        return FileTime.from(attrs.readLastModifiedTime())
    }

    override fun creationTime(): FileTime {
        return FileTime.from(attrs.readCreationTime())
    }

    override fun permissions(): Set<PosixFilePermission> {
        return attrs.readPermissions()
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