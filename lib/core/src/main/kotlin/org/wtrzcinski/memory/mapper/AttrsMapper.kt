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
import org.wtrzcinski.memory.buffer.AbstractMemoryReadWriteBuffer
import org.wtrzcinski.memory.mapper.schema.AttrsStructSchema
import org.wtrzcinski.memory.mode.Mode
import org.wtrzcinski.memory.mode.ModeMonitor
import org.wtrzcinski.memory.mode.OpenMode
import org.wtrzcinski.memory.util.Check
import java.nio.file.attribute.PosixFilePermission
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@ExperimentalAtomicApi
@Suppress("unused")
class AttrsMapper(
    mode: Mode,
    private val context: MemorySegmentContext,
    private val schema: AttrsStructSchema,
    private val buffer: AbstractMemoryReadWriteBuffer,
    private var ref: BlockAddress? = null,
) : BlockBodyMapper, ModeMonitor(mode) {

    fun ref(): BlockAddress {
        return checkNotNull(ref)
    }

    fun readLastAccessTime(): Instant {
        throwIfNotReadable()
        val property = schema.lastAccessTime
        property.setPosition(buffer)
        return property.read(buffer)
    }

    fun writeLastAccessTime(lastAccessTime: Instant) {
        throwIfNotWritable()
        val property = schema.lastAccessTime
        property.write(buffer, lastAccessTime)
    }

    fun readLastModifiedTime(): Instant {
        throwIfNotReadable()
        val property = schema.lastModifiedTime
        property.setPosition(buffer)
        return property.read(buffer)
    }

    fun writeLastModifiedTime(lastModifiedTime: Instant) {
        throwIfNotWritable()
        val property = schema.lastModifiedTime
        property.write(buffer, lastModifiedTime)
    }

    fun readCreationTime(): Instant {
        throwIfNotReadable()
        val property = schema.creationTime
        property.setPosition(buffer)
        return property.read(buffer)
    }

    fun writeCreationTime(creationTime: Instant) {
        throwIfNotWritable()
        val property = schema.creationTime
        property.write(buffer, creationTime)
    }

    fun readPermissions(): Set<PosixFilePermission> {
        throwIfNotReadable()
        val property = schema.permissions
        property.setPosition(buffer)
        return property.read(buffer)
    }

    fun writePermissions(value: Set<PosixFilePermission>) {
        throwIfNotWritable()
        val property = schema.permissions
        property.setPosition(buffer)
        property.write(buffer, value)
    }

    fun writeOwner(owner: String) {
        throwIfNotWritable()
        val property = schema.owner
        property.write(buffer, owner)
    }

    fun writeGroup(group: String) {
        throwIfNotWritable()
        val property = schema.group
        property.write(buffer, group)
    }

    override fun flip(mode: OpenMode): BlockAddress {
        throwIfNotWritable()
        checkPosition(schema.offsetRange)

        if (tryFlip()) {
            try {
                buffer.flip()
                val exactBodySize = buffer.remaining()
                val directBuffer = context.ledger.allocateChannel(exactBodySize = exactBodySize)
                directBuffer.use {
                    it.write(source = buffer)
                }

                this.ref = directBuffer.address()
                return ref()
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
}