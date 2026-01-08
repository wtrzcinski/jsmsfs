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

package org.wtrzcinski.files.memory.mapper

import org.wtrzcinski.files.memory.MemorySegmentLedger
import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.schema.StructSchema
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.instantByteSize
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.intByteSize
import java.nio.file.attribute.PosixFilePermissions
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class MemoryMapperRegistry(val ledger: MemorySegmentLedger) {

    private val nodeSchema = StructSchema.builder()
        .field(name = "type", size = intByteSize)
        .field(name = "linkCount", size = intByteSize)
        .field(name = "data", size = ledger.addressSchema.handler.byteSize)
        .field(name = "attrs", size = ledger.addressSchema.handler.byteSize)
        .field(name = "name", size = ledger.addressSchema.handler.byteSize)
        .build()

    private val attrsSchema = StructSchema.builder()
        .field("lastAccessTime", size = instantByteSize)
        .field("lastModifiedTime", size = instantByteSize)
        .field("creationTime", size = instantByteSize)
        .field("permissions", size = intByteSize + ByteSize(9))
        .field("owner", minSize = intByteSize, maxSize = intByteSize + ByteSize(100 * 4))
        .field("group", minSize = intByteSize, maxSize = intByteSize + ByteSize(100 * 4))
        .build()

    fun createString(name: String): StringMapper {
        val nameMapper = StringMapper(memory = ledger)
        nameMapper.writeString(name)
        return nameMapper
    }

    fun createAttrs(): AttrsMapper {
        val now: Instant = Instant.ofEpochSecond(0, 0)
        val schema = attrsSchema
        val buffer = ledger.allocateChannel(maxBodySize = schema.maxBodySize())
        val mode = Mode.create()
        val attrsMapper = AttrsMapper(
            mappers = this,
            mode = mode,
            schema = schema,
            buffer = buffer
        )
        attrsMapper.writeLastAccessTime(now)
        attrsMapper.writeLastModifiedTime(now)
        attrsMapper.writeCreationTime(now)
        attrsMapper.writePermissions(PosixFilePermissions.fromString("rwx".repeat(3)))
        attrsMapper.writeOwner("")
        attrsMapper.writeGroup("")
        return attrsMapper
    }

    fun readAttrs(ref: BlockAddress): AttrsMapper {
        val mode = Mode.update()
        val lock = MemoryFileLock.unlocked(mode)
        val buffer = ledger.existingChannel(name = "", ref = ref, lock = lock)
        return AttrsMapper(
            mode = mode,
            schema = attrsSchema,
            mappers = this,
            buffer = buffer,
            ref = ref,
        )
    }

    fun createNode(): NodeMapper {
        val mode = Mode.create()
        val buffer = ledger.allocateChannel(exactBodySize = nodeSchema.bodySize())
        return NodeMapper(
            mode = mode,
            schema = nodeSchema,
            mappers = this,
            buffer = buffer,
        )
    }

    fun readNode(ref: BlockAddress): NodeMapper {
        val mode = Mode.update()
        val lock = MemoryFileLock.unlocked(mode)
        val channel = ledger.existingChannel(name = "", ref = ref, lock = lock)
        return NodeMapper(
            mode = mode,
            schema = nodeSchema,
            mappers = this,
            buffer = channel,
            ref = ref,
        )
    }
}