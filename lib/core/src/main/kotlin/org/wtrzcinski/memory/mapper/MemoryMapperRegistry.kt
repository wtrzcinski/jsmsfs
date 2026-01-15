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

package org.wtrzcinski.memory.mapper

import org.wtrzcinski.memory.MemorySegmentContext
import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.lock.MemoryFileLock
import org.wtrzcinski.memory.mapper.schema.AttrsStructSchema
import org.wtrzcinski.memory.mode.Mode
import java.nio.file.attribute.PosixFilePermissions
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class MemoryMapperRegistry(
    val context: MemorySegmentContext,
) {

    private val defaultPermissions = PosixFilePermissions.fromString("rwx".repeat(3))

    private val attrsSchema = AttrsStructSchema.attrsSchema(context.schemas)

    fun createString(name: String): StringMapper {
        val nameMapper = StringMapper(context.schemas, memory = context.ledger)
        nameMapper.writeString(name)
        return nameMapper
    }

    fun createAttrs(): AttrsMapper {
        val now: Instant = Instant.ofEpochSecond(0, 0)
        val schema = attrsSchema
        val buffer = context.ledger.allocateChannel(maxBodySize = schema.maxBodySize())
        val mode = Mode.create()
        val attrsMapper = AttrsMapper(
            context = context,
            mode = mode,
            schema = schema,
            buffer = buffer
        )
        attrsMapper.writeLastAccessTime(now)
        attrsMapper.writeLastModifiedTime(now)
        attrsMapper.writeCreationTime(now)
        attrsMapper.writePermissions(defaultPermissions)
        attrsMapper.writeOwner("")
        attrsMapper.writeGroup("")
        return attrsMapper
    }

    fun readAttrs(ref: BlockAddress): AttrsMapper {
        val mode = Mode.update()
        val lock = MemoryFileLock.unlocked(mode)
        val buffer = context.ledger.existingChannel(ref = ref, lock = lock)
        return AttrsMapper(
            context = context,
            mode = mode,
            schema = attrsSchema,
            buffer = buffer,
            ref = ref,
        )
    }

    fun createNode(): NodeMapper {
        val nodeSchema = context.schemas.nodeSchema
        val mode = Mode.create()
        val buffer = context.ledger.allocateChannel(exactBodySize = nodeSchema.bodySize())
        return NodeMapper(
            context = context,
            mode = mode,
            schema = nodeSchema,
            buffer = buffer,
        )
    }

    fun readNode(ref: BlockAddress): NodeMapper {
        val nodeSchema = context.schemas.nodeSchema
        val mode = Mode.update()
        val lock = MemoryFileLock.unlocked(mode)
        val channel = context.ledger.existingChannel(ref = ref, lock = lock)
        return NodeMapper(
            context = context,
            mode = mode,
            schema = nodeSchema,
            buffer = channel,
            ref = ref,
        )
    }
}