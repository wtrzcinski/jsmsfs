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

import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.ModeMonitor
import org.wtrzcinski.files.memory.mode.OpenMode
import org.wtrzcinski.files.memory.schema.StructSchema
import org.wtrzcinski.files.memory.util.Check
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@ExperimentalAtomicApi
@Suppress("unused")
class AttrsMapper(
    mode: Mode,
    private val mappers: MemoryMapperRegistry,
    private val schema: StructSchema,
    private val buffer: MemoryReadWriteBuffer,
    private var ref: BlockAddress? = null,
) : BlockBodyMapper, ModeMonitor(mode) {

    fun ref(): BlockAddress {
        return checkNotNull(ref)
    }

    fun readLastAccessTime(): Instant {
        throwIfNotReadable()
        setPosition(schema.offsetRange("lastAccessTime"))

        return buffer.readInstant()
    }

    fun writeLastAccessTime(lastAccessTime: Instant) {
        throwIfNotWritable()
        checkPosition(schema.offsetRange("lastAccessTime"))

        buffer.writeInstant(lastAccessTime)
    }

    fun readLastModifiedTime(): Instant {
        throwIfNotReadable()
        setPosition(schema.offsetRange("lastModifiedTime"))

        return buffer.readInstant()
    }

    fun writeLastModifiedTime(lastModifiedTime: Instant) {
        throwIfNotWritable()
        checkPosition(schema.offsetRange("lastModifiedTime"))

        buffer.writeInstant(lastModifiedTime)
    }

    fun readCreationTime(): Instant {
        throwIfNotReadable()
        setPosition(schema.offsetRange("creationTime"))

        return buffer.readInstant()
    }

    fun writeCreationTime(creationTime: Instant) {
        throwIfNotWritable()
        checkPosition(schema.offsetRange("creationTime"))

        buffer.writeInstant(creationTime)
    }

    fun writePermissions(value: Set<PosixFilePermission>) {
        throwIfNotWritable()
        checkPosition(schema.offsetRange("permissions"))

        buffer.writeString(PosixFilePermissions.toString(value))
    }

    fun readPermissions(): Set<PosixFilePermission> {
        throwIfNotReadable()
        setPosition(schema.offsetRange("permissions"))

        val readString = buffer.readString()
        return PosixFilePermissions.fromString(readString)
    }

    fun writeOwner(owner: String) {
        throwIfNotWritable()
        checkPosition(schema.offsetRange("owner"))

        buffer.writeString(owner)
    }

    fun writeGroup(group: String) {
        throwIfNotWritable()
        checkPosition(schema.offsetRange("group"))

        buffer.writeString(group)
    }

    override fun flip(mode: OpenMode): BlockAddress {
        throwIfNotWritable()
        checkPosition(schema.offsetRange)

        if (tryFlip()) {
            try {
                buffer.flip()
                val exactBodySize = buffer.remaining()
                val directBuffer = mappers.ledger.allocateChannel(exactBodySize = exactBodySize)
                directBuffer.use {
                    it.write(source = buffer)
                }

                this.ref = directBuffer.address()
                return checkNotNull(this.ref)
            } finally {
                buffer.close()
                buffer.release()
            }
        } else {
            throwIllegalStateException()
        }
    }

    private fun checkPosition(range: ClosedRange<BlockAddress>) {
        Check.isTrue { BlockAddress(buffer.position()) in range }
    }

    private fun setPosition(range: ClosedRange<BlockAddress>) {
        require(range.start == range.endInclusive)
        buffer.position(range.start.start)
    }
}