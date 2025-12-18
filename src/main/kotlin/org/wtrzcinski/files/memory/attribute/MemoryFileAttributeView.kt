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
import org.wtrzcinski.files.memory.node.ValidNode
import java.nio.ByteBuffer
import java.nio.file.attribute.*

internal class MemoryFileAttributeView(
    val fileSystem: MemorySegmentFileSystem,
    val name: String,
    val node: ValidNode,
): PosixFileAttributeView, UserDefinedFileAttributeView {
    override fun name(): String {
        return name
    }

    override fun setTimes(lastModifiedTime: FileTime, lastAccessTime: FileTime, createTime: FileTime) {
        fileSystem.updateFileTime(node.nodeRef, attrs = AttributesBlock(
            lastModifiedTime = lastModifiedTime.toInstant(),
            lastAccessTime = lastAccessTime.toInstant(),
            creationTime = createTime.toInstant()
        ))
    }

    override fun setPermissions(permissions: Set<PosixFilePermission>) {
        fileSystem.updatePermissions(node.nodeRef, attrs = AttributesBlock(permissions = permissions))
    }

    override fun readAttributes(): PosixFileAttributes {
        return MemoryFileAttributes(
            fileSystem = fileSystem,
            name = name,
            node = node,
            attrs = fileSystem.findAttrs(node),
        )
    }

    override fun getOwner(): UserPrincipal? {
        TODO("Not yet implemented")
    }

    override fun setOwner(owner: UserPrincipal?) {
        TODO("Not yet implemented")
    }

    override fun setGroup(group: GroupPrincipal?) {
        TODO("Not yet implemented")
    }

    override fun list(): List<String?>? {
        TODO("Not yet implemented")
    }

    override fun size(name: String): Int {
        TODO("Not yet implemented")
    }

    override fun read(name: String?, dst: ByteBuffer?): Int {
        TODO("Not yet implemented")
    }

    override fun write(name: String?, src: ByteBuffer?): Int {
        TODO("Not yet implemented")
    }

    override fun delete(name: String?) {
        TODO("Not yet implemented")
    }
}