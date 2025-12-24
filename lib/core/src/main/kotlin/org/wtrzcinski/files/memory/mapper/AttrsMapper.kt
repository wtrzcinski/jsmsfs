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

import org.wtrzcinski.files.memory.MemoryLedger
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryByteBuffer
import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.instantByteSize
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.intByteSize
import org.wtrzcinski.files.memory.util.AbstractCloseable
import org.wtrzcinski.files.memory.util.Preconditions.requireTrue
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.time.Instant
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@ExperimentalAtomicApi
internal class AttrsMapper(
    private val memory: MemoryLedger,
): BlockBodyMapper, AbstractCloseable() {
    companion object {
        val instantsSize: ByteSize = instantByteSize * 3
        val permissionsSize: ByteSize = intByteSize + ByteSize(9)
        val maxStringSize: ByteSize = intByteSize + ByteSize(100 * 4)
        private val minSize: Long = (instantByteSize * 3).size + permissionsSize.size + (intByteSize.size * 2)
        private val maxSize: ByteSize = (instantByteSize * 3) + permissionsSize + (maxStringSize * 2)
    }

    private val tmpBuffer: MemoryByteBuffer

    init {
        tmpBuffer = memory.heapBuffer(maxSize)
    }

    fun writeLastAccessTime(lastAccessTime: Instant) {
        checkIsOpen()
        requireTrue(tmpBuffer.position() == 0L)

        tmpBuffer.writeInstant(lastAccessTime)
    }

    fun writeLastModifiedTime(lastModifiedTime: Instant) {
        checkIsOpen()
        requireTrue(tmpBuffer.position() == instantByteSize.size)

        tmpBuffer.writeInstant(lastModifiedTime)
    }

    fun writeCreationTime(creationTime: Instant) {
        checkIsOpen()
        requireTrue(tmpBuffer.position() == (instantByteSize * 2).size)

        tmpBuffer.writeInstant(creationTime)
    }

    fun writePermissions(value: Set<PosixFilePermission>) {
        checkIsOpen()
        requireTrue(tmpBuffer.position() == instantsSize.size)

        tmpBuffer.writeString(PosixFilePermissions.toString(value))
    }

    fun writeOwner(owner: String) {
        checkIsOpen()
        requireTrue(tmpBuffer.position() == instantsSize.size + permissionsSize.size)

        tmpBuffer.writeString(owner)
    }

    fun writeGroup(group: String) {
        checkIsOpen()
        requireTrue(tmpBuffer.position() >= instantsSize.size + permissionsSize.size + 4)

        tmpBuffer.writeString(group)
    }

    fun size(): Long {
        checkIsOpen()
        requireTrue(tmpBuffer.position() >= minSize)

        return tmpBuffer.position()
    }

    override fun flip(): BlockStart {
        if (tryClose()) {
            requireTrue(tmpBuffer.position() >= minSize)

            tmpBuffer.flip()
            val directBuffer = memory.newByteChannel(bodySize = tmpBuffer.remaining())
            directBuffer.use {
                it.write(value = tmpBuffer)
            }
            tmpBuffer.release()
            return directBuffer.first()
        }
        throw MemoryIllegalStateException()
    }
}