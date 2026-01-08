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

package org.wtrzcinski.files.memory.schema

import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer
import java.lang.Short
import kotlin.Long

/**
 * @see java.lang.invoke.VarHandle
 */
interface ValueHandler<T> {
    companion object {
        val byteSize: ByteSize = ByteSize(value = Byte.SIZE_BYTES.toLong())

        val shortByteSize: ByteSize = ByteSize(value = kotlin.Short.SIZE_BYTES.toLong())

        val intByteSize: ByteSize = ByteSize(value = Int.SIZE_BYTES.toLong())

        val longByteSize: ByteSize = ByteSize(value = Long.SIZE_BYTES.toLong())

        val instantByteSize: ByteSize = longByteSize + intByteSize

        const val InvalidRef: Long = -1

        val MaxUnsignedShortInclusive: Long = Short.toUnsignedLong(-1) - 1L

        val UnsignedShortRange: LongRange = 0..MaxUnsignedShortInclusive

        val MaxUnsignedIntInclusive: Long = Integer.toUnsignedLong(-1) - 1L

        val UnsignedIntRange: LongRange = 0..MaxUnsignedIntInclusive
    }

    val byteSize: ByteSize

    fun read(buffer: MemoryReadWriteBuffer): T

    fun write(buffer: MemoryReadWriteBuffer, value: T)

}