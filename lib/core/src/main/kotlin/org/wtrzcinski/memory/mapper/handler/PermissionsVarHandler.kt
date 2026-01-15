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

package org.wtrzcinski.memory.mapper.handler

import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.buffer.MemoryReadWriteBuffer
import java.nio.file.attribute.PosixFilePermission
import kotlin.experimental.and
import kotlin.experimental.or

/**
 * @see java.nio.file.attribute.PosixFilePermissions.fromString
 * @see java.nio.file.attribute.PosixFilePermissions.toString
 */
class PermissionsVarHandler : SimpleVarHandler<Set<PosixFilePermission>> {
    companion object {
        private val r: Byte = 1.toByte()
        private val w: Byte = (1 shl 1).toByte()
        private val x: Byte = (1 shl 2).toByte()
        private val no: Byte = 0.toByte()
    }

    override fun byteSize(): DefaultBlockSize {
        return DefaultBlockSize(3)
    }

    override fun read(buffer: MemoryReadWriteBuffer): Set<PosixFilePermission> {
        val result = mutableSetOf<PosixFilePermission>()
        val owner = buffer.readByte()
        if ((owner and r) == r) {
            result.add(PosixFilePermission.OWNER_READ)
        }
        if ((owner and w) == w) {
            result.add(PosixFilePermission.OWNER_WRITE)
        }
        if ((owner and x) == x) {
            result.add(PosixFilePermission.OWNER_EXECUTE)
        }

        val group = buffer.readByte()
        if ((group and r) == r) {
            result.add(PosixFilePermission.GROUP_READ)
        }
        if ((group and w) == w) {
            result.add(PosixFilePermission.GROUP_WRITE)
        }
        if ((group and x) == x) {
            result.add(PosixFilePermission.GROUP_EXECUTE)
        }

        val other = buffer.readByte()
        if ((other and r) == r) {
            result.add(PosixFilePermission.OTHERS_READ)
        }
        if ((other and w) == w) {
            result.add(PosixFilePermission.OTHERS_WRITE)
        }
        if ((other and x) == x) {
            result.add(PosixFilePermission.OTHERS_EXECUTE)
        }
        return result
    }

    override fun write(buffer: MemoryReadWriteBuffer, value: Set<PosixFilePermission>) {
        var owner: Byte = no
        if (value.contains(PosixFilePermission.OWNER_READ)) {
            owner = owner or r
        }
        if (value.contains(PosixFilePermission.OWNER_WRITE)) {
            owner = owner or w
        }
        if (value.contains(PosixFilePermission.OWNER_EXECUTE)) {
            owner = owner or x
        }
        buffer.writeByte(owner)

        var group: Byte = no
        if (value.contains(PosixFilePermission.GROUP_READ)) {
            group = group or r
        }
        if (value.contains(PosixFilePermission.GROUP_WRITE)) {
            group = group or w
        }
        if (value.contains(PosixFilePermission.GROUP_EXECUTE)) {
            group = group or x
        }
        buffer.writeByte(group)

        var other: Byte = no
        if (value.contains(PosixFilePermission.OTHERS_READ)) {
            other = other or r
        }
        if (value.contains(PosixFilePermission.OTHERS_WRITE)) {
            other = other or w
        }
        if (value.contains(PosixFilePermission.OTHERS_EXECUTE)) {
            other = other or x
        }
        buffer.writeByte(other)
    }
}