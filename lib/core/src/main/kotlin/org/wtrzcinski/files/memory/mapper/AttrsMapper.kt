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
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.channel.FragmentedReadWriteBuffer
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.instantByteSize
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.intByteSize
import org.wtrzcinski.files.memory.mode.AbstractCloseable
import org.wtrzcinski.files.memory.util.Check
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@ExperimentalAtomicApi
class AttrsMapper(
    private val name: String,
    private val memory: MemorySegmentLedger,
) : BlockBodyMapper, AbstractCloseable() {

    companion object {
        val instantsSize: ByteSize = instantByteSize * 3
        val permissionsSize: ByteSize = intByteSize + ByteSize(9)
        val maxStringSize: ByteSize = intByteSize + ByteSize(100 * 4)
        private val minSize: Long = (instantByteSize * 3).size + permissionsSize.size + (intByteSize.size * 2)
        private val maxSize: ByteSize = (instantByteSize * 3) + permissionsSize + (maxStringSize * 2)
    }

    private val tmpBuffer: FragmentedReadWriteBuffer = memory.newBuffer(name = "$name.tmp", bodyAlignment = maxSize)

    fun writeLastAccessTime(lastAccessTime: Instant) {
        checkIsWritable()
        Check.isTrue { tmpBuffer.position() == 0L }

        tmpBuffer.writeInstant(lastAccessTime)
    }

    fun writeLastModifiedTime(lastModifiedTime: Instant) {
        checkIsWritable()
        Check.isTrue { tmpBuffer.position() == instantByteSize.size }

        tmpBuffer.writeInstant(lastModifiedTime)
    }

    fun writeCreationTime(creationTime: Instant) {
        checkIsWritable()
        Check.isTrue { tmpBuffer.position() == (instantByteSize * 2).size }

        tmpBuffer.writeInstant(creationTime)
    }

    fun writePermissions(value: Set<PosixFilePermission>) {
        checkIsWritable()
        Check.isTrue { tmpBuffer.position() == instantsSize.size }

        tmpBuffer.writeString(PosixFilePermissions.toString(value))
    }

    fun writeOwner(owner: String) {
        checkIsWritable()
        Check.isTrue { tmpBuffer.position() == instantsSize.size + permissionsSize.size }

        tmpBuffer.writeString(owner)
    }

    fun writeGroup(group: String) {
        checkIsWritable()
        Check.isTrue { tmpBuffer.position() >= instantsSize.size + permissionsSize.size + 4 }

        tmpBuffer.writeString(group)
    }

    fun size(): Long {
        checkIsReadable()
        Check.isTrue { tmpBuffer.position() >= minSize }

        return tmpBuffer.position()
    }

    override fun flip(): BlockStart {
        if (tryFlip()) {
            Check.isTrue { tmpBuffer.position() >= minSize }

            try {
                tmpBuffer.flip()
                val bodySize = tmpBuffer.remaining()
                val directBuffer = memory.newBuffer(name = "$name.direct", bodyAlignment = bodySize, capacity = bodySize)
                directBuffer.use {
                    it.write(value = tmpBuffer)
                }

                val result = directBuffer.first()
                return result
            } finally {
                tmpBuffer.close()
                tmpBuffer.release()
            }
        } else {
            throwIllegalStateException()
        }
    }
}