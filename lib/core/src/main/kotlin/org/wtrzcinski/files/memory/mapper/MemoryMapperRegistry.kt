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

package org.wtrzcinski.files.memory.mapper

import org.wtrzcinski.files.memory.MemorySegmentLedger
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.node.AttributesBlock
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class MemoryMapperRegistry(val memory: MemorySegmentLedger) {
    companion object {
        val intByteSize: ByteSize = ByteSize(value = Int.SIZE_BYTES.toLong())
        val longByteSize: ByteSize = ByteSize(value = Long.SIZE_BYTES.toLong())
        val instantByteSize: ByteSize = longByteSize + intByteSize
    }

    fun createName(name: String): NameMapper {
        val nameMapper = NameMapper(memory = memory)
        nameMapper.writeName(name)
        return nameMapper
    }

    fun createAttrs(name: String): AttrsMapper {
        val attrs = AttributesBlock(now = Instant.now())
        val attrsMapper = AttrsMapper(memory = memory, name = name)
        attrsMapper.writeLastAccessTime(attrs.lastAccessTime)
        attrsMapper.writeLastModifiedTime(attrs.lastModifiedTime)
        attrsMapper.writeCreationTime(attrs.creationTime)
        attrsMapper.writePermissions(attrs.permissions)
        attrsMapper.writeOwner(attrs.owner)
        attrsMapper.writeGroup(attrs.group)
        return attrsMapper
    }

    fun createFile(name: String): NodeMapper {
        val result = NodeMapper(memory = memory, name = name, mode = Mode.readWrite())
        return result
    }
}