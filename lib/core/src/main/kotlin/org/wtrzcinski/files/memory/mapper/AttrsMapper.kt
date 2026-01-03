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
import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.ModeState
import org.wtrzcinski.files.memory.schema.MapperSchema
import org.wtrzcinski.files.memory.util.Check
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@ExperimentalAtomicApi
class AttrsMapper(
    mode: Mode,
    private val memory: MemorySegmentLedger,
    private val schema: MapperSchema,
) : BlockBodyMapper, ModeState(mode) {

    private val buffer: MemoryReadWriteBuffer = memory.allocateChannel(bodyAlignment = schema.bodyAlignment())

    fun writeLastAccessTime(lastAccessTime: Instant) {
        checkIsWritable()
        checkPosition(schema.offsetRange("lastAccessTime"))

        buffer.writeInstant(lastAccessTime)
    }

    fun writeLastModifiedTime(lastModifiedTime: Instant) {
        checkIsWritable()
        checkPosition(schema.offsetRange("lastModifiedTime"))

        buffer.writeInstant(lastModifiedTime)
    }

    fun writeCreationTime(creationTime: Instant) {
        checkIsWritable()
        checkPosition(schema.offsetRange("creationTime"))

        buffer.writeInstant(creationTime)
    }

    fun writePermissions(value: Set<PosixFilePermission>) {
        checkIsWritable()
        checkPosition(schema.offsetRange("permissions"))

        buffer.writeString(PosixFilePermissions.toString(value))
    }

    fun writeOwner(owner: String) {
        checkIsWritable()
        checkPosition(schema.offsetRange("owner"))

        buffer.writeString(owner)
    }

    fun writeGroup(group: String) {
        checkIsWritable()
        checkPosition(schema.offsetRange("group"))

        buffer.writeString(group)
    }

    override fun flip(): BlockOffset {
        checkIsWritable()
        checkPosition(schema.offsetRange)

        if (tryFlip()) {
            try {
                buffer.flip()
                val bodySize = buffer.remaining()
                val directBuffer = memory.allocateChannel(bodySize = bodySize)
                directBuffer.use {
                    it.write(source = buffer)
                }

                return directBuffer.address()
            } finally {
                buffer.close()
                buffer.release()
            }
        } else {
            throwIllegalStateException()
        }
    }

    private fun checkPosition(range: ClosedRange<BlockOffset>) {
        Check.isTrue { BlockOffset(buffer.position()) in range }
    }

}